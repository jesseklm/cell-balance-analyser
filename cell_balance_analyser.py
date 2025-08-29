import asyncio
import logging
import signal

import httpcore
from httpcore import ConnectError

from config import get_first_config
from mqtt_handler import MqttHandler

__version__ = '0.0.6'


class CellBalanceAnalyser:
    def __init__(self) -> None:
        self.config = get_first_config()
        self.setup_logging()
        self.metric_url = self.config.get('victoriametrics_prom_import_url')
        self.mqtt_handler: MqttHandler = MqttHandler(self.config, self.handle_mqtt_message)
        self.balance_times = dict()

    def setup_logging(self):
        if 'logging' in self.config:
            logging_level_name: str = self.config['logging'].upper()
            logging_level: int = logging.getLevelNamesMapping().get(logging_level_name, logging.NOTSET)
            if logging_level != logging.NOTSET:
                logging.getLogger().setLevel(logging_level)
            else:
                logging.warning(f'unknown logging level: %s.', logging_level)

    async def connect(self):
        while not await self.mqtt_handler.connect():
            await asyncio.sleep(1)

    async def handle_mqtt_message(self, topic: str, payload: str):
        if topic.startswith('esp-module/') and topic.endswith('/balance_request'):
            topic = topic[len('esp-module/'):]
            module = topic[:topic.index('/')]
            topic = topic[topic.index('/') + 1:]
            topic = topic[len('cell/'):]
            cell = topic[:topic.index('/')]
            try:
                balance_time = round(int(payload) / 1000)
                module = int(module)
                cell = int(cell)
            except ValueError:
                return
            if module not in self.balance_times:
                self.balance_times[module] = dict()
            if cell not in self.balance_times[module]:
                self.balance_times[module][cell] = 0
            self.balance_times[module][cell] += balance_time
            self.mqtt_handler.publish(f'{module}/{cell}/balance_time', self.balance_times[module][cell], retain=True)
            if self.metric_url:
                await self.send_metric(f'balance_time{{module="{module}",cell="{cell}"}}',
                                       self.balance_times[module][cell])
        elif topic.startswith(self.mqtt_handler.topic_prefix) and topic.endswith('/balance_time'):
            topic = topic[len(self.mqtt_handler.topic_prefix):]
            module = topic[:topic.index('/')]
            topic = topic[topic.index('/') + 1:]
            cell = topic[:topic.index('/')]
            try:
                balance_time = int(payload)
                module = int(module)
                cell = int(cell)
            except ValueError:
                return
            if module not in self.balance_times:
                self.balance_times[module] = dict()
            self.balance_times[module][cell] = balance_time
        else:
            logging.info('%s > %s', topic, payload)

    async def exit(self):
        await self.mqtt_handler.disconnect()

    async def send_metric(self, metric_name: str, value: str) -> str:
        async with httpcore.AsyncConnectionPool() as http:
            try:
                content = f'{metric_name} {value}'
                response = await http.request(
                    method='POST',
                    url=self.metric_url,
                    content=content.encode()
                )
                return response.content.decode()
            except ConnectError as e:
                logging.warning(f'{e=}, {content=}')
            except Exception as e:
                logging.error(f'{e=}, {content=}')
            return ''


async def main():
    cell_balance_analyser = CellBalanceAnalyser()
    await cell_balance_analyser.connect()
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()

    def shutdown_handler():
        if not main_task.done():
            main_task.cancel()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_handler)
    except NotImplementedError:
        pass
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        logging.info('exiting.')
    finally:
        await cell_balance_analyser.exit()
        logging.info('exited.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('gmqtt').setLevel(logging.ERROR)
    logging.info(f'starting Cell Balance Analyser v%s.', __version__)
    asyncio.run(main())
