# Instrukcja komunikacji klienta z serwerem gry (RabbitMQ)

## Wymagania

- Python 3.8+
- Zainstalowany RabbitMQ (domyślnie na `localhost`)

## Uruchamianie serwera

Serwer uruchamiamy z parametrem określającym strefę (ZONE):

```
python server.py forest
```
lub
```
python server.py desert
```

Domyślna strefa to `forest`, jeśli nie podasz argumentu.

## Uruchamianie klienta

Klient może być uruchomiony w dowolnej liczbie instancji. Każdy klient powinien posiadać unikalny `PLAYER_ID`.

## Kolejki i exchange

- **EXCHANGE_C2S** (`game.client_to_server`): klient → serwer (direct)
- **EXCHANGE_M2C** (`game.movement_to_client`): serwer → klient (topic, pozycje graczy)
- **EXCHANGE_A2C** (`game.animations_to_client`): serwer → klient (topic, animacje)
- **EXCHANGE_I2C** (`game.interactions_to_client`): serwer → klient (topic, interakcje)
- **EXCHANGE_PT** (`game.player_transfer`): transfery graczy między serwerami

## Typy wiadomości i routing_key

### 1. Dołączanie do gry (JOIN)

**Wysyłane przez klienta:**

- **exchange:** `game.client_to_server`
- **routing_key:** `join.{ZONE}`

```json
{
  "playerId": "twoj_player_id"
}
```

### 2. Opuszczanie gry (LEAVE)

**Wysyłane przez klienta:**

- **exchange:** `game.client_to_server`
- **routing_key:** `leave.{ZONE}`

```json
{
  "playerId": "twoj_player_id"
}
```

### 3. Wysyłanie pozycji (MOVE)

**Wysyłane przez klienta cyklicznie:**

- **exchange:** `game.client_to_server`
- **routing_key:** `movement.{ZONE}`

```json
{
  "id": "twoj_player_id",
  "posX": 1.0,
  "posY": 0.0,
  "posZ": 2.0,
  "rotX": 0,
  "rotY": 90,
  "rotZ": 0,
  "sclX": 1,
  "sclY": 1,
  "sclZ": 1,
  "timestamp": 1719060000
}
```

### 4. Wysyłanie interakcji

**Wysyłane przez klienta:**

- **exchange:** `game.client_to_server`
- **routing_key:** `interactions.{ZONE}`

```json
{
  "playerId": "twoj_player_id",
  "interaction": "jump",
  "started": true,
  "timestamp": 1719060000
}
```

### 5. Wysyłanie animacji

**Wysyłane przez klienta:**

- **exchange:** `game.client_to_server`
- **routing_key:** `animations.{ZONE}`

```json
{
  "playerId": "twoj_player_id",
  "animation": "wave",
  "timestamp": 1719060000
}
```

### 6. Transfer gracza między serwerami

**Wysyłane przez klienta (lub serwer):**

- **exchange:** `game.client_to_server`
- **routing_key:** `transfer.{ZONE}`

```json
{
  "playerId": "twoj_player_id",
  "from": "forest",
  "to": "desert",
  "stanZdrowia": 80,
  "cytryny": 5,
  "igly": 0,
  "timestamp": 1719060000
}
```

## Odbieranie wiadomości przez klienta

Klient powinien nasłuchiwać na swoje dedykowane kolejki (każdy gracz ma własny routing_key):

- **Pozycje graczy:**  
  - **exchange:** `game.movement_to_client`
  - **routing_key:** `movement.{PLAYER_ID}`

- **Animacje:**  
  - **exchange:** `game.animations_to_client`
  - **routing_key:** `animations.{PLAYER_ID}`

- **Interakcje:**  
  - **exchange:** `game.interactions_to_client`
  - **routing_key:** `interactions.{PLAYER_ID}`

## Przykładowy cykl życia klienta

1. Klient wysyła JOIN.
2. Klient cyklicznie wysyła MOVE, co jakiś czas INTERACTION i ANIMATION.
3. Klient odbiera aktualizacje pozycji, animacji i interakcji z serwera.
4. Klient może wysłać transfer do innej strefy.
5. Klient wysyła LEAVE przy zamknięciu.

## Przykład uruchomienia klienta

```python
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
            body=json.dumps(leave)
        )
        time.sleep(1)
        print("Zamykanie klienta...")
        exit(0)
```

## Uwaga

- Każdy klient powinien mieć unikalny `PLAYER_ID`.
- Każdy klient powinien nasłuchiwać tylko na swoje kolejki (routing_key z własnym `PLAYER_ID`).
- Po stronie serwera nie należy współdzielić połączeń/kanałów RabbitMQ między wątkami.

---
**Przykładowy kod klienta i serwera znajdziesz w plikach `test_client2.py` oraz `server.py`.**