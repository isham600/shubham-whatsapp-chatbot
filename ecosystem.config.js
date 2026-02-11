module.exports = {
  apps: [
    {
      name: "whatsapp_chatbot",
      script: "app.js",
      cwd: "/var/www/html/official_whatsapp_chatbot/whatsapp-chatbot",
      instances: 1,
      exec_mode: "fork",
      watch: false,
      env: {
        NODE_ENV: "production",
        PORT: 7700
      }
    }
  ]
}
