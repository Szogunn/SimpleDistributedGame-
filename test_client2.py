import pika
import json
import time
import random
import threading
import string
import os

RABBITMQ_HOST = 'localhost'
ZONE = 'desert'  # Strefa, w której działa klient
EXCHANGE_C2S = 'game.client_to_server'
EXCHANGE_I2C = 'game.interactions_to_client'
EXCHANGE_A2C = 'game.animations_to_client'

PLAYER_ID = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))

def send_position():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    while True:
        pos = {
            "id": PLAYER_ID,
            "posX": random.uniform(-10, 10),
            "posY": 0,
            "posZ": random.uniform(-10, 10),
            "rotX": 0,
            "rotY": random.uniform(0, 360),
            "rotZ": 0,
            "sclX": 1,
            "sclY": 1,
            "sclZ": 1,
            "timestamp": time.time()
        }
        channel.basic_publish(
            exchange=EXCHANGE_C2S,
            routing_key=f"movement.{ZONE}",
            body=json.dumps(pos)
        )
        time.sleep(1)

def send_interaction():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    while True:
        time.sleep(5)
        interaction = {
            "type": "action",
            "playerId": PLAYER_ID,
            "interaction": random.choice(["jump", "run", "attack"]),
            "started": True,
            "timestamp": time.time()
        }
        channel.basic_publish(
            exchange=EXCHANGE_C2S,
            routing_key=f"interactions.{ZONE}",
            body=json.dumps(interaction)
        )

def send_animation():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    while True:
        time.sleep(7)
        animation = {
            "playerId": PLAYER_ID,
            "animation": random.choice(["wave", "dance", "sit"]),
            "timestamp": time.time()
        }
        channel.basic_publish(
            exchange=EXCHANGE_C2S,
            routing_key=f"animations.{ZONE}",
            body=json.dumps(animation)
        )
        
# Exchange do odbierania pozycji graczy       
EXCHANGE_M2C = 'game.movement_to_client'

# Prosta plansza 21x21, środek to (0,0)
BOARD_SIZE = 21
OFFSET = BOARD_SIZE // 2

# Mapa: player_id -> (x, z)
players = {}

# Lock do synchronizacji dostępu do zasobników graczy
players_lock = threading.Lock()

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

# Słowniki do przechowywania ostatnich interakcji i animacji graczy
last_interactions = {}
last_animations = {}

def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            updates = msg.get('updates', [])
            for update in updates:
                pid = update['id']
                pos = update['position']
                with players_lock:
                    players[pid] = (pos['x'], pos['y'], pos['z'])
        except Exception as e:
            print("Błąd dekodowania:", e)
            
def animations(ch, method, properties, body):
    try:
        msg = json.loads(body)
        pid = msg.get('playerId')
        animation = msg.get('animation')
        with players_lock:
            last_animations[pid] = animation
    except Exception as e:
        print("Błąd dekodowania:", e)

def interactions(ch, method, properties, body):
    try:
        msg = json.loads(body)
        pid = msg.get('playerId')
        interaction = msg.get('interaction')
        with players_lock:
            if interaction == 'left':
                if pid in players:
                    del players[pid]
            else:
                last_interactions[pid] = interaction
    except Exception as e:
        print("Błąd dekodowania:", e)

def consume_positions():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    movement_queue = channel.queue_declare(queue='', exclusive=True)
    movement_queue_name = movement_queue.method.queue
    channel.queue_bind(exchange='game.movement_to_client', queue=movement_queue_name, routing_key=f"movement.{PLAYER_ID}")
    
    animations_queue = channel.queue_declare(queue='', exclusive=True)
    animations_queue_name = animations_queue.method.queue
    channel.queue_bind(exchange='game.animations_to_client', queue=animations_queue_name, routing_key=f"animations.{PLAYER_ID}")
    
    interactions_queue = channel.queue_declare(queue=f'interactions.{PLAYER_ID}', exclusive=True)
    interactions_queue_name = interactions_queue.method.queue
    channel.queue_bind(exchange='game.interactions_to_client', queue=interactions_queue_name, routing_key=f"interactions.{PLAYER_ID}")

    channel.basic_consume(queue=movement_queue_name, on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue=animations_queue_name, on_message_callback=animations, auto_ack=True)
    channel.basic_consume(queue=interactions_queue_name, on_message_callback=interactions, auto_ack=True)
    channel.start_consuming()


def send_transfer():
    global ZONE
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    while True:
        time.sleep(30)
        with players_lock:
            players.clear()  # Wyczyść listę graczy przed transferem
        new_zone = 'desert' if ZONE == 'forest' else 'forest'
        transfer_msg = {
            "playerId": PLAYER_ID,
            "from": ZONE,
            "to": new_zone,
            "stanZdrowia": random.randint(50, 100),
            "cytryny": random.randint(0, 10),
            "igly": random.randint(0, 5),
            "timestamp": int(time.time())
        }
        channel.basic_publish(
            exchange=EXCHANGE_C2S,
            routing_key=f"transfer.{ZONE}",
            body=json.dumps(transfer_msg)
        )
        
        ZONE = new_zone

def draw_board():
    board = [['.' for _ in range(BOARD_SIZE)] for _ in range(BOARD_SIZE)]
    with players_lock:
        players_copy = dict(players)
        last_interactions_copy = dict(last_interactions)
        last_animations_copy = dict(last_animations)
    for pid, pos in players_copy.items():
        x, z = int(round(pos[0])), int(round(pos[2]))
        bx = x + OFFSET
        bz = z + OFFSET
        if 0 <= bx < BOARD_SIZE and 0 <= bz < BOARD_SIZE:
            board[bz][bx] = pid[0].upper()  # pierwsza litera id gracza
    clear_screen()
    print(f"Plansza (środek to (0,0)): {ZONE}")
    for row in reversed(board):
        print(' '.join(row))
    print("\nGracze:")
    for pid, pos in players_copy.items():
        print(f"{pid}: x={pos[0]:.2f}, z={pos[2]:.2f}")
        # Wyświetlanie ostatniej interakcji
        if pid in last_interactions_copy:
            print(f"  Ostatnia interakcja: {last_interactions_copy[pid]}")
        # Wyświetlanie ostatniej animacji
        if pid in last_animations_copy:
            print(f"  Ostatnia animacja: {last_animations_copy[pid]}")


def main():    
    threading.Thread(target=send_position, daemon=True).start()
    threading.Thread(target=send_interaction, daemon=True).start()
    threading.Thread(target=send_animation, daemon=True).start()
    threading.Thread(target=send_transfer, daemon=True).start()
    threading.Thread(target=consume_positions, daemon=True).start()
    while True:
        draw_board()
        time.sleep(0.3)

if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    join = {        
            "playerId": PLAYER_ID,            
        }
    channel.basic_publish(
            exchange=EXCHANGE_C2S,
            routing_key=f"join.{ZONE}",
            body=json.dumps(join)
        )
    
    time.sleep(1)
    try:
        main()
    except KeyboardInterrupt:
        leave = {        
            "playerId": PLAYER_ID,            
        }
        channel.basic_publish(
            exchange=EXCHANGE_C2S,
            routing_key=f"leave.{ZONE}",
            body=json.dumps(join)
        )
        time.sleep(1)
    
        print("Zamykanie klienta...")
        exit(0)