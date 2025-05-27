import aio_pika
import json
import asyncio
from datetime import datetime
from queue import Queue

class GameClient:
    def __init__(self, player_id, message_queue: asyncio.Queue):
        self.player_id = player_id
        self.connection = None
        self.channel = None
        self.message_queue = message_queue
        self.url = "amqp://guest:guest@localhost/"
        self.queue_name = None
        self.current_realm = None  
        self.connection_ready = asyncio.Event() 
        self.player_input_exchange = None

    async def send_move(self, target_x, target_y):
        """Wysyła ruch gracza do RabbitMQ."""
        if not self.channel or self.channel.is_closed:
            print("Brak połączenia z RabbitMQ. Nie można wysłać ruchu.")
            return

        message = {
            "action": "move",
            "player_id": self.player_id,
            "x": target_x,
            "y": target_y            
        }
        
        await self.player_input_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=f"player_input_{self.current_realm}"  # Użycie klucza routingu z krainą
        )
        
    async def join(self, realm):
        """Wysyła informację o dołączeniu gracza do gry."""
        if not self.channel or self.channel.is_closed:
            print("Brak połączenia z RabbitMQ. Nie można wysłać ruchu.")
            return

        await self.queue_name.bind(exchange="game_updates", routing_key=f"updates.{realm}")     
        self.current_realm = realm        
        message = {
            "action": "player_join",
            "player_id": self.player_id,               
        }
                 # Aktualizacja bieżącej krainy
        await self.player_input_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=f"player_input_{realm}"  # Użycie klucza routingu z krainą
        )   
    
    async def left(self):
        """Wysyła informację o dołączeniu gracza do gry."""
        if not self.channel or self.channel.is_closed:
            print("Brak połączenia z RabbitMQ. Nie można wysłać ruchu.")
            return

        await self.queue_name.unbind(exchange="game_updates", routing_key=f"updates.{self.current_realm}")
        message = {
            "action": "player_left",
            "player_id": self.player_id,               
        }
        
        await self.player_input_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=f"player_input_{self.current_realm}"  # Użycie klucza routingu z krainą
        )       
        
    async def on_message_received(self, message: aio_pika.IncomingMessage):
        """Callback dla odbieranych wiadomości."""
        async with message.process():
            message_data = json.loads(message.body)
            action = message_data.get("action") 
            if "game_state" == action: 
                
                print(message_data)
            if "move" == action: 
                await self.message_queue.put({
                    "type": "move",
                    "player_id": message_data["player_id"],
                    "x": message_data["x"],
                    "y": message_data["y"]
                })
                                    
            elif "player_left" == action:                
                await self.message_queue.put({
                    "type": "player_left",
                    "player_id": message_data["player_id"]
                })

            elif "player_join" == action:  
                players = message_data["players"]                               
                for player_id, player_data in players.items():
                    await self.message_queue.put({
                        "type": "move",
                        "player_id": player_id,
                        "x": player_data["x"],
                        "y": player_data["y"]})                                     

    async def setup(self):
        """Konfiguracja połączenia, wymian i kolejek."""
        try:
            self.connection = await aio_pika.connect_robust(self.url)
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)            

            exchange = await self.channel.declare_exchange('game_updates', aio_pika.ExchangeType.TOPIC)
            self.queue_name = await self.channel.declare_queue(exclusive=True)
            # await self.queue_name.bind(exchange, routing_key=f"updates.{self.current_realm}")        
            await self.queue_name.bind(exchange, routing_key=f"updates.{self.player_id}")
            await self.queue_name.consume(self.on_message_received)

            self.player_input_exchange = await self.channel.declare_exchange("player_input_exchange", aio_pika.ExchangeType.DIRECT)
            print(f"[{self.player_id}] Połączono z RabbitMQ.")
            self.connection_ready.set() 
            
            await asyncio.Future()            
        except aio_pika.exceptions.AMQPConnectionError as e:
            print(f"[{self.player_id}] Błąd połączenia: {e}. Ponowne łączenie za 5 sekund...")
            await asyncio.sleep(5)
            await self.setup()
        
    async def switch_realm(self):
        await self.left()
        new_realm = "desert" if self.current_realm == "forest" else "forest"
        await self.join(new_realm)
        

    async def stop(self):
        """Zamyka połączenie."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            print(f"[{self.player_id}] Połączenie zamknięte.")