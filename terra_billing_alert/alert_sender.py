import logging
from slack import WebClient
from slack.errors import SlackApiError


logger = logging.getLogger(__name__)


class AlertSender:
    def send_message(self, title, message):
        print(title)
        print(message)


class SlackSender(AlertSender):
    def __init__(self, channel, token):
        self.client = WebClient(token)
        self.channel = channel
        self.token = token

    def send_message(self, title, message):
        try:
            self.client.chat_postMessage(
                channel=self.channel,
                text='\n'.join([title, message])
            )
        except SlackApiError as e:
            logger.error(f'Failed to send slack message due to error {e.response["error"]}')
