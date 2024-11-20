import discord
from discord.ext import commands
import os
import logging

logger = logging.getLogger(__name__)

class WelcomeBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True  # Need this for member updates
        intents.message_content = True
        
        super().__init__(command_prefix='!', intents=intents)
        
    async def setup_hook(self):
        logger.info(f'{self.user} has connected to Discord!')

    @commands.Cog.listener()
    async def on_member_update(self, before, after):
        # ID of the role you want to track
        target_role_id = int(os.environ.get('DISCORD_ROLE_ID'))
        
        # Check if the target role was added
        if any(role.id == target_role_id for role in after.roles) and not any(role.id == target_role_id for role in before.roles):
            try:
                welcome_message = (
                    f"Welcome to Home of Fight Picks, {after.mention}! 🎉\n\n"
                    "Thank you for joining our premium membership. Here's what you can expect:\n"
                    "• Access to our exclusive market view\n"
                    "• Real-time updates on fight odds\n"
                    "• Premium analysis and insights\n\n"
                    "Head to https://homeoffightpicks.com to enter the HOF portal!"
                )
                
                # Try to DM the user
                try:
                    await after.send(welcome_message)
                    logger.info(f"Sent welcome DM to {after.name}")
                except discord.Forbidden:
                    # If DM fails, try to send in a welcome channel
                    welcome_channel_id = int(os.environ.get('DISCORD_GUILD_ID'))
                    channel = self.get_channel(welcome_channel_id)
                    if channel:
                        await channel.send(welcome_message)
                        logger.info(f"Sent welcome message in channel for {after.name}")
                    else:
                        logger.error("Could not find welcome channel")
                
            except Exception as e:
                logger.error(f"Error sending welcome message: {e}")

def run_bot():
    bot = WelcomeBot()
    
    @bot.event
    async def on_ready():
        logger.info(f'Bot is ready: {bot.user.name}')
        
    token = os.environ.get('DISCORD_BOT_TOKEN')
    if not token:
        raise ValueError("No bot token found in environment variables")
        
    bot.run(token)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_bot()