import pika, json, threading, time
import threading
import sys
players_lock = threading.Lock()

RABBITMQ_HOST = 'localhost'
# Pobierz ZONE z argumentów wiersza poleceń lub ustaw domyślnie
if len(sys.argv) > 1:
    ZONE = sys.argv[1]
else:
    ZONE = 'forest'  # Domyślna strefa

EXCHANGE_C2S = 'game.client_to_server'
EXCHANGE_M2C = 'game.movement_to_client'
EXCHANGE_A2C = 'game.animations_to_client'
EXCHANGE_I2C = 'game.interactions_to_client'
EXCHANGE_PT = 'game.player_transfer'

MOVEMENT_SERVER_QUEUE = f"movement_server_queue_{ZONE}"
INTERACTIONS_SERVER_QUEUE = f"interactions_server_queue_{ZONE}"
ANIMATIONS_SERVER_QUEUE = f"animations_server_queue_{ZONE}"
JOIN_SERVER_QUEUE = f"join_server_queue_{ZONE}"
LEAVE_SERVER_QUEUE = f"leave_server_queue_{ZONE}"
PLAYER_TRANSFER_SERVER_QUEUE = f"player_transfer_{ZONE}"
TRANSFER_TO_SERVER_QUEUE = f"transfer_to_server_{ZONE}"

TICK_INTERVAL = 0.1  # 100 ms

# Mapa: player_id -> stan gracza; nadpisanie tego samego player_id aktualizuje dane
players_movement = {}  # player_id -> {'position': (x,y,z), 'rotation':..., 'last_update': timestamp}

# Połączenie RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_C2S, exchange_type='direct')
channel.exchange_declare(exchange=EXCHANGE_A2C, exchange_type='topic')
channel.exchange_declare(exchange=EXCHANGE_I2C, exchange_type='topic')
channel.exchange_declare(exchange=EXCHANGE_PT, exchange_type='topic')

channel.queue_declare(queue=MOVEMENT_SERVER_QUEUE, durable=False)
channel.queue_bind(exchange=EXCHANGE_C2S, queue=MOVEMENT_SERVER_QUEUE, routing_key=f"movement.{ZONE}")

channel.queue_declare(queue=INTERACTIONS_SERVER_QUEUE, durable=False)
channel.queue_bind(exchange=EXCHANGE_C2S, queue=INTERACTIONS_SERVER_QUEUE, routing_key=f"interactions.{ZONE}")

channel.queue_declare(queue=ANIMATIONS_SERVER_QUEUE, durable=False)
channel.queue_bind(exchange=EXCHANGE_C2S, queue=ANIMATIONS_SERVER_QUEUE, routing_key=f"animations.{ZONE}")

channel.queue_declare(queue=JOIN_SERVER_QUEUE, durable=False)
channel.queue_bind(exchange=EXCHANGE_C2S, queue=JOIN_SERVER_QUEUE, routing_key=f"join.{ZONE}")

channel.queue_declare(queue=LEAVE_SERVER_QUEUE, durable=False)
channel.queue_bind(exchange=EXCHANGE_C2S, queue=LEAVE_SERVER_QUEUE, routing_key=f"leave.{ZONE}")

channel.queue_declare(queue=PLAYER_TRANSFER_SERVER_QUEUE, durable=False)
channel.queue_bind(exchange=EXCHANGE_C2S, queue=PLAYER_TRANSFER_SERVER_QUEUE, routing_key= f"transfer.{ZONE}")

channel.queue_declare(queue=TRANSFER_TO_SERVER_QUEUE, durable=False)
channel.queue_bind(exchange=EXCHANGE_PT, queue=TRANSFER_TO_SERVER_QUEUE, routing_key= f"transfer.*.{ZONE}")

def add_player_to_map(pid):
    with players_lock:
        if pid in players_movement:
            print(f"Gracz {pid} już istnieje, aktualizuję jego dane.")
            players_movement[pid]['last_update'] = time.time()
        else:
            players_movement[pid] = {
                'position': (0, 0, 0),
                'rotationY': 0,
                'last_update': time.time()
            }
            print(f"Gracz {pid} dołączył do gry (join).")

def remove_player_from_map(pid):
    with players_lock:
        if pid in players_movement:
            del players_movement[pid]
            
            print(f"Gracz {pid} opuścił grę (leave).")
             # (opcjonalnie: powiadom innych graczy, jak było wcześniej)
            players = players_movement.keys()
            response = {
            "playerId": pid,
            "interaction": 'left',
            "timestamp": time.time()
            }
            for player_id in players:
                channel.basic_publish(
                    exchange=EXCHANGE_I2C,
                    routing_key=f'interactions.{player_id}',
                    body=json.dumps(response)
                )
            
        else:
            print(f"Gracz {pid} nie jest aktywny, ignoruję wiadomość leave.")

def on_client_join_message(ch, method, properties, body):
    try:
        msg = json.loads(body)
        pid = msg.get("id") or msg.get("playerId")
        if not pid:
            print("Brak ID gracza w wiadomości join.")
            return
        add_player_to_map(pid)
    except Exception as e:
        print("Błąd w on_client_join_message:", e)

def on_client_leave_message(ch, method, properties, body):
    try:
        msg = json.loads(body)
        pid = msg.get("id") or msg.get("playerId")
        if not pid:
            print("Brak ID gracza w wiadomości leave.")
            return
        remove_player_from_map(pid)       
    except Exception as e:
        print("Błąd w on_client_leave_message:", e)

def on_client_move_message(ch, method, properties, body):
    active_players = get_active_players()
    
    with players_lock:        
        try:       
            msg = json.loads(body)                   
            pid = msg.get('id')
            if pid not in active_players:
                print(f"Gracz {pid} nie jest aktywny, ignoruję wiadomość.")
                return            

            pos = {
                'x': msg.get('posX', 0),
                'y': msg.get('posY', 0),
                'z': msg.get('posZ', 0)
            }
            rotation_y = msg.get('rotY', 0)

            ts = msg.get('timestamp', time.time())                    
            players_movement[pid] = {
                'position': (pos['x'], pos['y'], pos['z']), # Pozycja X,Y,Z
                'rotationY': rotation_y, # Rotacja Y
                'last_update': ts
            }
        except Exception as e:
            print("Błąd w on_client_message:", e)
            
def on_client_interaction_message(ch, method, properties, body):
    try:
        msg = json.loads(body)
        response = {
            "playerId": msg.get("playerId"),
            "interaction": msg.get("interaction"),
            "timestamp": time.time()
        }
        
        players = get_active_players()
        for player_id in players:
            channel.basic_publish(
                exchange=EXCHANGE_I2C,
                routing_key=f'interactions.{player_id}',
                body=json.dumps(response)
            )
        
    except Exception as e:
        print("Błąd w on_client_interaction_message:", e)

def on_client_animation_message(ch, method, properties, body):
    try:
        msg = json.loads(body)
        response = {
            "playerId": msg.get("playerId"),
            "animation": msg.get("animation"),
            "timestamp": time.time()
        }

        players = get_active_players()
        for player_id in players:
            channel.basic_publish(
                exchange=EXCHANGE_A2C,
                routing_key=f"animations.{player_id}",
                body=json.dumps(response)
            )
        
    except Exception as e:
        print("Błąd w on_client_animation_message:", e)
        
def on_client_player_transfer_message(ch, method, properties, body):
    try:
        
        msg = json.loads(body)
        player_id = msg.get("playerId")
        remove_player_from_map(player_id)
        response = {
            "playerId": player_id,
            "from": msg.get("from"),
            "to": msg.get("to"),
            "timestamp": msg.get("timestamp", time.time())
        }
        
        print(f"Przesyłam transfer gracza {response['playerId']} z {response['from']} do {response['to']}")
        target = msg.get("to")
        player_id = msg.get("playerId"),
        channel.basic_publish(
            exchange=EXCHANGE_PT,
            routing_key=f"transfer.{ZONE}.{target}",
            body=json.dumps(response)
        )    
    except Exception as e:
        print("Błąd w on_client_player_transfer_message:", e)
        
def on_server_player_transfer_message(ch, method, properties, body):
    try:
        msg = json.loads(body)
        player_id = msg.get("playerId")
        add_player_to_map(player_id)
        
    except Exception as e:
        print("Błąd w on_server_player_transfer_message:", e)

def start_server_consume():
    channel.basic_consume(queue=MOVEMENT_SERVER_QUEUE, on_message_callback=on_client_move_message, auto_ack=True)
    channel.basic_consume(queue=INTERACTIONS_SERVER_QUEUE, on_message_callback=on_client_interaction_message, auto_ack=True)
    channel.basic_consume(queue=ANIMATIONS_SERVER_QUEUE, on_message_callback=on_client_animation_message, auto_ack=True)
    channel.basic_consume(queue=PLAYER_TRANSFER_SERVER_QUEUE, on_message_callback=on_client_player_transfer_message, auto_ack=True)
    channel.basic_consume(queue=TRANSFER_TO_SERVER_QUEUE, on_message_callback=on_server_player_transfer_message, auto_ack=True)
    channel.basic_consume(queue=JOIN_SERVER_QUEUE, on_message_callback=on_client_join_message, auto_ack=True)
    channel.basic_consume(queue=LEAVE_SERVER_QUEUE, on_message_callback=on_client_leave_message, auto_ack=True)
    channel.start_consuming()
    
def get_active_players():
    with players_lock:
        return players_movement.keys()

def build_players_json():
    """
    Zwraca JSON-a z aktualnym stanem wszystkich graczy.
    """
    with players_lock:
        players = []  # Upewnij się, że klucze są aktualne
        updates = []
        for pid, movement in players_movement.items():
            players.append(pid)  # Dodajemy ID gracza do listy
            
            pos = movement.get('position', (0, 0, 0))
            rotY = movement.get('rotationY', 0)
            updates.append({
                'id': pid,
                'position': {'x': pos[0], 'y': pos[1], 'z': pos[2]},
                'rotationY': rotY,
                'timestamp': movement.get('last_update')
            })
        #print(f"build_players_json: players={players}, updates={updates}")
        return players, json.dumps({'updates': updates}) if updates else None


def tick_broadcast():
    # Każdy wątek powinien mieć własne połączenie i kanał!
    connection_mov = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel_mov = connection_mov.channel()
    channel_mov.exchange_declare(exchange=EXCHANGE_M2C, exchange_type='topic')
    try:
        while True:
            players, body = build_players_json()
            if body is not None :
                #print(f"tick_broadcast: sending to players={players}, body={body}")
                for player_id in players:
                    channel_mov.basic_publish(
                        exchange=EXCHANGE_M2C,
                        routing_key=f'movement.{player_id}',
                        body=body
                    )
            #else:
                #print("tick_broadcast: no updates to send")
            time.sleep(TICK_INTERVAL)
    except Exception as e:
        print(f"Error in tick_broadcast: {e}")
    finally:
        channel_mov.close()
        connection_mov.close()

if __name__ == '__main__':
    def cleanup_inactive():
        while True:
            now = time.time()
            for pid, state in list(players_movement.items()):
                if now - state['last_update'] > 30:  # np. 30s bez update → usuń
                    del players_movement[pid]
            time.sleep(10)
    
    # threading.Thread(target=cleanup_inactive, daemon=True).start()
    threading.Thread(target=tick_broadcast, daemon=True).start()
    start_server_consume()
