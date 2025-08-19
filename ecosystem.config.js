// ecosystem.config.js
require('dotenv').config();

module.exports = {
  apps: [
    {
      name: 'nostr-relay',
      script: 'dist/src/index.js',
      instances: 1, // Start with 1, can increase based on your server specs
      exec_mode: 'cluster',
      watch: false,
      max_memory_restart: '1G',
      env_file: '.env', // PM2 will load this .env file
      env: {
        NODE_ENV: 'production',
        DEBUG_COLORS: 'false',
        FORCE_COLOR: '0',
        // You can override specific vars here or leave empty
        // PM2 will load everything from .env automatically
      },
      error_file: './logs/err.log',
      out_file: './logs/out.log',
      log_file: './logs/combined.log',
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      // Auto-restart settings
      min_uptime: '10s',
      max_restarts: 10,
      restart_delay: 4000,
      // Graceful shutdown
      kill_timeout: 5000,
      wait_ready: true,
      listen_timeout: 3000,
    }
  ]
};
