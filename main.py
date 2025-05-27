import asyncio
import aio_pika
import json
import random
from datetime import datetime

def build_move_message (player_id, x, y):
        return{
        "action": "move",
        "player_id": player_id,
        "x": x,
        "y": y
        }

class GameServer:
    def __init__(self, server_id, realm, host="localhost", item_spawn_interval=5, voting_set=None):
        self.url = f"amqp://guest:guest@{host}/"
        self.connection = None
        self.channel = None
        self.item_spawn_interval = item_spawn_interval  # Interwał generowania przedmiotów (sekundy)
        self.item_counter = 0  # Licznik do generowania unikalnych ID przedmiotów
        self.server_id = server_id
        self.realm = realm 
        self.lock = asyncio.Lock()
        self.game_state = {
            "players": {}  # {player_id: {"x": int, "y": int, "last_updated": str}}
            }

        self.voting_set = voting_set if voting_set else []
        self.request_queue = asyncio.Queue()
        self.granted = set()
        self.requesting_cs = False
        
        
    async def on_message_received(self, message: aio_pika.IncomingMessage):        
        async with message.process():
            try:        
                message_data = json.loads(message.body)
                action = message_data["action"]
                if "move" == action:
                    async with self.lock:
                        player_id = message_data["player_id"]
                        self.game_state["players"][player_id] = {
                        "x": message_data["x"],
                        "y": message_data["y"],
                        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                                            
                    move_message = build_move_message(player_id, message_data["x"], message_data["y"])                     
                    exchange = await self.channel.get_exchange("game_updates")
                    await exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(move_message).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        ),
                        routing_key=f"updates.{self.realm}"
                    )
                    
                elif action == "player_join":
                    async with self.lock:                        
                        player_id = message_data["player_id"]                        
                        player_data = self.game_state["players"].get(player_id, None)
                        if player_data is None:
                            self.game_state["players"][player_id] = {
                            "x": 400,
                            "y": 300,
                            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                        else:
                            print(f"[Server] Gracz {player_id} już istnieje w stanie gry. BŁĄD!")
                            return
                        
                    player_join_msg = {
                        "action": "player_join",
                        "player_id": player_id,
                        "realm": self.realm,
                        "players": self.game_state["players"]
                    } 
                    exchange = await self.channel.get_exchange("game_updates")
                    await exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(player_join_msg).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        ),
                        routing_key=f"updates.{self.realm}"
                    )
                    
                    sync_msg = {
                        "action": "player_join",
                        "player_id": player_id,
                        "realm": self.realm,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                        
                    sync_exchange = await self.channel.get_exchange("sync_exchange")                   
                    await sync_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(sync_msg).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                            routing_key=""
                    )
                
                elif action == "player_left":
                    async with self.lock:                        
                        player_id = message_data["player_id"]
                        player_data = self.game_state["players"].pop(player_id, None)
                        if player_data:
                            message = {
                                "action": "player_left",
                                "player_id": player_id,
                            }
                        else:
                            print(f"[Server] Gracz {player_id} nie istnieje w stanie gry. BŁĄD!")
                            return
                    
                    exchange = await self.channel.get_exchange("game_updates")
                    await exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(message).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        ),
                        routing_key=f"updates.{self.realm}"
                    )                                            
            except json.JSONDecodeError as e:
                print(f"[Server] Błąd dekodowania JSON: {e}")
            except Exception as e:
                print(f"[Server] Błąd przetwarzania wiadomości: {e}")
        

    async def on_sync_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            try:
                data = json.loads(message.body)
                action = data.get("action")
                player_id = data.get("player_id")
                realm = data.get("realm")
                # timestamp = data.get("timestamp") # Możesz użyć, jeśli potrzebne

                print(f"[Serwer {self.server_id}] Otrzymano wiadomość synchronizacyjną: {data}")

                # W tym modelu serwer głównie dba o swoją krainę.
                # Wiadomości synchronizacyjne służą głównie do logowania lub
                # potencjalnie do budowania globalnego obrazu (jeśli byłoby to potrzebne w przyszłości).
                # Kluczowa logika zmiany krainy jest napędzana przez klienta (left -> join).

                if action == "player_join": # Wiadomość, że gracz dołączył do krainy (na innym serwerze)
                    if realm != self.realm:
                        print(f"[Serwer {self.server_id}] Gracz {player_id} dołączył do krainy {realm} (obsługiwanej przez inny serwer).")
                    # Ten serwer nie musi modyfikować swojego self.game_state["players"],
                    # chyba że gracz dołącza do JEGO krainy, co jest obsługiwane przez on_message_received.

                elif action == "player_left_realm": # Wiadomość, że gracz opuścił krainę (na innym serwerze)
                    if realm != self.realm:
                        print(f"[Serwer {self.server_id}] Gracz {player_id} opuścił krainę {realm} (obsługiwaną przez inny serwer).")

                # Można tu dodać logikę np. aktualizacji globalnej listy graczy, jeśli serwery
                # miałyby utrzymywać taki stan dla np. globalnych czatów, list znajomych itp.
                # Dla obecnych wymagań, logowanie może wystarczyć.

            except json.JSONDecodeError as e:
                print(f"[Serwer {self.server_id}] Błąd dekodowania JSON w on_sync_message: {e}")
            except Exception as e:
                print(f"[Serwer {self.server_id}] Błąd w on_sync_message: {e}")



    async def setup(self):
        """Konfiguracja połączenia, wymian i kolejek."""
        try:
            # Połączenie z RabbitMQ
            self.connection = await aio_pika.connect_robust(self.url)
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)

            # Deklaracja wymiany i kolejki dla wejścia
            await self.channel.declare_exchange("player_input_exchange", aio_pika.ExchangeType.DIRECT)
            queue = await self.channel.declare_queue(f"player_input_{self.realm}", durable=True)
            await queue.bind("player_input_exchange", routing_key=f"player_input_{self.realm}")

            # Deklaracja wymiany dla aktualizacji gry (fanout)
            await self.channel.declare_exchange("game_updates", aio_pika.ExchangeType.TOPIC)
            
            # Deklaracja wymiany i kolejki dla synchronizacji serwerów
            await self.channel.declare_exchange("sync_exchange", aio_pika.ExchangeType.FANOUT)
            sync_queue = await self.channel.declare_queue(f"sync_{self.server_id}", exclusive=True)
            await sync_queue.bind("sync_exchange", routing_key=f"sync.{self.server_id}")

            # Rozpoczęcie konsumowania wiadomości
            await queue.consume(self.on_message_received)
            await sync_queue.consume(self.on_sync_message)
            print(f"[Server {self.server_id}] Połączono z RabbitMQ.")

        except aio_pika.exceptions.AMQPConnectionError as e:
            print(f"[Server] Błąd połączenia: {e}. Ponowne łączenie za 5 sekund...")
            await asyncio.sleep(5)
            await self.setup()


    async def run(self):
        """Główna pętla serwera."""
        try:
            # Uruchom konfigurację w tle
            setup_task = asyncio.create_task(self.setup())

            # Generowanie przedmiotów w pętli
            while True:
                await asyncio.sleep(self.item_spawn_interval)                
        except KeyboardInterrupt:
            print("[Server] Zamykanie serwera...")
        except Exception as e:
            print(f"[Server] Błąd w pętli serwera: {e}")
        finally:
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                print("[Server] Połączenie zamknięte.")
                setup_task.cancel()

async def main():
    server1 = GameServer("server_1", "forest", item_spawn_interval=5)
    server2 = GameServer("server_2", "desert", item_spawn_interval=5)
    await asyncio.gather(server1.run(), server2.run())

if __name__ == "__main__":
    asyncio.run(main())