import uuid

# Funkcja pomocnicza do zwracania koloru gracza
def get_player_color(player_id):
    """Zwraca kolor dla gracza na podstawie ID."""
    colors = [(255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0)]
    return colors[hash(player_id) % len(colors)]

# Klasa zarządzająca stanem gry
class GameState:
    def __init__(self):
        self.players_positions = {}  # {player_id: (x, y, color)}
        self.items = {}  # {item_id: (x, y, item_type)}

    def update_player_position(self, player_id, x, y):
        """Aktualizuje pozycję gracza."""
        self.players_positions[player_id] = (x, y, get_player_color(player_id))
        
    def remove_player(self, player_id):
        self.players_positions.pop(player_id, None)

    def get_players(self):
        """Zwraca aktualne pozycje graczy."""
        return self.players_positions

    def clear_playerss(self):
        print("Clearing players positions")
        return self.players_positions.clear()
    
    def add_item(self, x, y, item_type):
        item_id = str(uuid.uuid4())
        self.items[item_id] = (x, y, item_type)
        return item_id

    def remove_item(self, item_id):
        self.items.pop(item_id, None)
    
    def get_items(self):
        return self.items