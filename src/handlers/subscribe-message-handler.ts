import { anyPass, equals, isNil, map, propSatisfies, uniqWith } from 'ramda'
import b4a from 'b4a'
import { Fabric } from '@spacesprotocol/fabric'
import { pipeline } from 'stream/promises'

import { createEndOfStoredEventsNoticeMessage, createNoticeMessage, createOutgoingEventMessage } from '../utils/messages'
import { IAbortable, IMessageHandler } from '../@types/message-handlers'
import { isEventMatchingFilter, toNostrEvent } from '../utils/event'
import { streamEach,  streamFilter, streamMap } from '../utils/stream'
import { SubscriptionFilter, SubscriptionId } from '../@types/subscription'
import { createLogger } from '../factories/logger-factory'
import { Event } from '../@types/event'
import { IEventRepository } from '../@types/repositories'
import { IWebSocketAdapter } from '../@types/adapters'
import { Settings } from '../@types/settings'
import { SubscribeMessage } from '../@types/messages'
import { WebSocketAdapterEvent } from '../constants/adapter'

const debug = createLogger('subscribe-message-handler')

export class SubscribeMessageHandler implements IMessageHandler, IAbortable {
  //private readonly abortController: AbortController

  public constructor(
    private readonly webSocket: IWebSocketAdapter,
    private readonly eventRepository: IEventRepository,
    private readonly settings: () => Settings,
    private readonly fabric: Fabric
  ) {
    //this.abortController = new AbortController()
    this.fabric = fabric
  }

  public abort(): void {
    //this.abortController.abort()
  }

  public async handleMessage(message: SubscribeMessage): Promise<void> {
    const subscriptionId = message[1]
    const filters = uniqWith(equals, message.slice(2)) as SubscriptionFilter[]

    const reason = this.canSubscribe(subscriptionId, filters)
    if (reason) {
      debug('subscription %s with %o rejected: %s', subscriptionId, filters, reason)
      this.webSocket.emit(WebSocketAdapterEvent.Message, createNoticeMessage(`Subscription rejected: ${reason}`))
      return
    }

    this.webSocket.emit(WebSocketAdapterEvent.Subscribe, subscriptionId, filters)

    await this.fetchAndSend(subscriptionId, filters)
  }

  private async fetchAndSend(subscriptionId: string, filters: SubscriptionFilter[]): Promise<void> {
    debug('fetching events for subscription %s with filters %o', subscriptionId, filters)
    
    const sendEvent = (event: Event) =>
      this.webSocket.emit(WebSocketAdapterEvent.Message, createOutgoingEventMessage(subscriptionId, event))
    const sendEOSE = () =>
      this.webSocket.emit(WebSocketAdapterEvent.Message, createEndOfStoredEventsNoticeMessage(subscriptionId))
    const isSubscribedToEvent = SubscribeMessageHandler.isClientSubscribedToEvent(filters)

    // First, fetch from local database
    const findEvents = this.eventRepository.findByFilters(filters).stream()

    try {
      await pipeline(
        findEvents,
        streamFilter(propSatisfies(isNil, 'deleted_at')),
        streamMap(toNostrEvent),
        streamFilter(isSubscribedToEvent),
        streamEach(sendEvent),
      )
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        debug('subscription %s aborted: %o', subscriptionId, error)
        findEvents.destroy()
      } else {
        debug('error streaming events from database: %o', error)
      }
      throw error
    }

    await this.fetchFromFabric(subscriptionId, filters, sendEvent, isSubscribedToEvent)

    sendEOSE()
  }

  private async fetchFromFabric(
    subscriptionId: string, 
    filters: SubscriptionFilter[], 
    sendEvent: (event: Event) => void,
    isSubscribedToEvent: (event: Event) => boolean
  ): Promise<void> {
    debug('fetching events from Fabric for subscription %s', subscriptionId)

    try {
      await this.queryFabricByFilters(filters, sendEvent, isSubscribedToEvent)
    } catch (error) {
      debug('error fetching from Fabric: %o', error)
      // Don't throw here - we want to continue even if Fabric fails
    }
  }

  private async queryFabricByFilters(
    filters: SubscriptionFilter[],
    sendEvent: (event: Event) => void,
    isSubscribedToEvent: (event: Event) => boolean
  ): Promise<void> {
    for (const filter of filters) {
      // Extract spaces from #s tag
      const spaceTags = filter['#s'] 
      const spaces = spaceTags ? (Array.isArray(spaceTags) ? spaceTags : [spaceTags]) : []

      console.log(spaces)
      
      // Extract d tags from #d tag
      const dTagValues = filter['#d']
      const dTags = dTagValues ? (Array.isArray(dTagValues) ? dTagValues : [dTagValues]) : ['']

      console.log(dTags)
      
      // Get authors/pubkeys
      const authors = filter.authors || []

      console.log(authors)
      
      // Get kinds to query - use all kinds from filter, or common Fabric kinds if none specified
      const kinds = filter.kinds //

      console.log(kinds)

      // Query by spaces first (if any)
      for (const space of spaces) {
        for (const kind of kinds) {
          for (const dTag of dTags) {
            try {
              await this.queryFabricEvent(space, kind, dTag, sendEvent, isSubscribedToEvent, filter)
            } catch (error) {
              debug('error querying Fabric for space %s, kind %d, d %s: %o', space, kind, dTag, error)
            }
          }
        }
      }
      
      // Query by authors/pubkeys (if any)
      for (const author of authors) {
        for (const kind of kinds) {
          for (const dTag of dTags) {
            try {
              await this.queryFabricEvent(author, kind, dTag, sendEvent, isSubscribedToEvent, filter)
            } catch (error) {
              debug('error querying Fabric for author %s, kind %d, d %s: %o', author, kind, dTag, error)
            }
          }
        }
      }
      
      // If no spaces or authors specified, skip Fabric query
      if (spaces.length === 0 && authors.length === 0) {
        debug('Fabric query skipped: no spaces or authors specified in filter')
      }
    }
  }

  private extractDTagsFromFilter(filter: SubscriptionFilter): string[] {
    if (!filter['#d']) return []
    return Array.isArray(filter['#d']) ? filter['#d'] : [filter['#d']]
  }

  private async queryFabricEvent(
    spaceOrPubkey: string,
    kind: number,
    d: string,
    sendEvent: (event: Event) => void,
    isSubscribedToEvent: (event: Event) => boolean,
    filter: SubscriptionFilter
  ): Promise<void> {
    try {
      const opts: any = {}
      
      // Add created_at filter if specified
      if (filter.since) {
        opts.created_at = filter.since
      }

      const fabricResult = await this.fabric.eventGet(spaceOrPubkey, kind, d, opts)

      console.log('fabr result', fabricResult)

      if (fabricResult && fabricResult.event) {
        const convertedEvent = this.convertFabricEventToNostrEvent(fabricResult)
        
        // Additional filtering based on until timestamp
        if (filter.until && convertedEvent.created_at > filter.until) {
          return
        }
        
        // Check if the event matches the subscription filters
        if (isSubscribedToEvent(convertedEvent)) {
          debug('sending Fabric event: %s', convertedEvent.id)
          sendEvent(convertedEvent)
        }
      }
    } catch (error) {
      debug('error fetching event from Fabric (%s, %d, %s): %o', spaceOrPubkey, kind, d, error)
    }
  }

  private convertFabricEventToNostrEvent(fabricResult: any): Event {
    const fabricEvent = fabricResult.event

    debug('fabric event', fabricEvent)
    
    // Convert binary data to hex strings
    const pubkey = fabricEvent.pubkey instanceof Buffer || fabricEvent.pubkey instanceof Uint8Array 
      ? b4a.toString(fabricEvent.pubkey, 'hex')
      : fabricEvent.pubkey

    const sig = fabricEvent.sig instanceof Buffer || fabricEvent.sig instanceof Uint8Array
      ? b4a.toString(fabricEvent.sig, 'hex') 
      : fabricEvent.sig

    // Convert content from Uint8Array to string
    let content = ''
    if (fabricEvent.content instanceof Uint8Array || fabricEvent.content instanceof Buffer) {
      content = fabricEvent.binary_content 
        ? b4a.toString(fabricEvent.content, 'base64')
        : b4a.toString(fabricEvent.content, 'utf-8')
    } else {
      content = fabricEvent.content || ''
    }

    // Ensure all tag values are strings
    const normalizedTags = (fabricEvent.tags || []).map((tag: any[]) => 
      tag.map((value: any) => {
        if (value === null || value === undefined) return ''
        if (value instanceof Buffer || value instanceof Uint8Array) {
          return b4a.toString(value, 'hex')
        }
        return String(value)
      })
    )

    // Generate event ID if not present
    let eventId = fabricEvent.id
    if (!eventId) {
      // You might want to generate the ID based on the event content
      // For now, using a placeholder - you should implement proper ID generation
      eventId = 'generated-id-placeholder'
    } else if (eventId instanceof Buffer || eventId instanceof Uint8Array) {
      eventId = b4a.toString(eventId, 'hex')
    }
    
    // Convert CompactEvent format to Nostr Event format
    return {
      id: eventId,
      pubkey: pubkey,
      created_at: fabricEvent.created_at,
      kind: fabricEvent.kind,
      tags: normalizedTags,
      content: content,
      sig: sig,
      // Add any additional metadata from Fabric result if needed
      // Note: You might want to include closestNodes info somehow
    } as Event
  }

  private static isClientSubscribedToEvent(filters: SubscriptionFilter[]): (event: Event) => boolean {
    return anyPass(map(isEventMatchingFilter)(filters))
  }

  private canSubscribe(subscriptionId: SubscriptionId, filters: SubscriptionFilter[]): string | undefined {
    const subscriptions = this.webSocket.getSubscriptions()
    const existingSubscription = subscriptions.get(subscriptionId)
    const subscriptionLimits = this.settings().limits?.client?.subscription

    if (existingSubscription?.length && equals(filters, existingSubscription)) {
        return `Duplicate subscription ${subscriptionId}: Ignoring`
    }

    const maxSubscriptions = subscriptionLimits?.maxSubscriptions ?? 0
    if (maxSubscriptions > 0
      && !existingSubscription?.length && subscriptions.size + 1 > maxSubscriptions
    ) {
      return `Too many subscriptions: Number of subscriptions must be less than or equal to ${maxSubscriptions}`
    }

    const maxFilters = subscriptionLimits?.maxFilters ?? 0
    if (maxFilters > 0) {
      if (filters.length > maxFilters) {
        return `Too many filters: Number of filters per subscription must be less then or equal to ${maxFilters}`
      }
    }

    if (
      typeof subscriptionLimits.maxSubscriptionIdLength === 'number'
      && subscriptionId.length > subscriptionLimits.maxSubscriptionIdLength
    ) {
      return `Subscription ID too long: Subscription ID must be less or equal to ${subscriptionLimits.maxSubscriptionIdLength}`
    }
  }
}
