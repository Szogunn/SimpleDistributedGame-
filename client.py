import argparse
import pygame
import uuid
from GameClient_new import GameClient
import asyncio
from GameState import GameState

# Funkcja pomocnicza do zwracania koloru przedmiotu
def get_item_color(item_type):
    colors = {
        "coin": (255, 255, 0),  # Żółty dla monet
        "potion": (0, 255, 255)  # Cyjan dla mikstur
    }
    return colors.get(item_type, (128, 128, 128))

class GameManager:
    def __init__(self, player_id, screen_width=800, screen_height=600):
        self.player_id = player_id
        self.message_queue = asyncio.Queue()
        self.client = GameClient(player_id, self.message_queue)
        self.game_state = GameState()

        # Inicjalizacja Pygame
        pygame.init()
        self.screen = pygame.display.set_mode((screen_width, screen_height))
        pygame.display.set_caption(f"Gra - Gracz {player_id}")
        self.clock = pygame.time.Clock()

    async def handle_events(self):
        """Obsługuje zdarzenia Pygame."""
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return False
            elif event.type == pygame.MOUSEBUTTONDOWN:
                target_x, target_y = event.pos                
                await self.client.send_move(target_x, target_y)
            elif event.type == pygame.KEYDOWN:                
                if event.key == pygame.K_r:  # Zmiana krainy (R) 
                    self.game_state.clear_playerss()  # Czyści pozycje graczy           
                    await self.client.switch_realm()
        return True

    async def update_game_state(self):
        """Aktualizuje stan gry na podstawie wiadomości z kolejki."""
        while not self.message_queue.empty():
            message = await self.message_queue.get()
            
            if message["type"] == "move":
                self.game_state.update_player_position(
                    message["player_id"], message["x"], message["y"]
                )
                
            elif message["type"] == "item_added":
                self.game_state.add_item(
                    message["x"], message["y"], message["item_type"]
                )
            
            elif message["type"] == "player_left":
                self.game_state.remove_player(
                     message["player_id"]
                )

    def render(self):
        """Rysuje stan gry."""
        self.screen.fill((255, 255, 255))  # Białe tło
        
        # Rysuj przedmioty
        for item_id, (x, y, item_type) in self.game_state.get_items().items():
            color = get_item_color(item_type)
            if item_type == "coin":
                pygame.draw.circle(self.screen, color, (x, y), 5)
            elif item_type == "potion":
                pygame.draw.rect(self.screen, color, (x-5, y-5, 10, 10))
        
        # Rysuj graczy
        for pid, (x, y, color) in self.game_state.get_players().items():
            pygame.draw.circle(self.screen, color, (x, y), 10)
            font = pygame.font.SysFont(None, 24)
            text = font.render(pid[:8], True, (0, 0, 0))
            self.screen.blit(text, (x - 20, y - 30))
            
        pygame.display.flip()

    async def run(self, realm):
        """Główna pętla gry."""
        # Uruchom klienta w tle
        client_task = asyncio.create_task(self.client.setup())
        
        # Czekaj na zakończenie konfiguracji RabbitMQ
        await self.client.connection_ready.wait()
        # Czekaj na zakończenie konfiguracji RabbitMQ        
        await self.client.join(realm)  # Dołącz gracza do gry

        running = True
        while running:
            # Obsługa zdarzeń
            running = await self.handle_events()
            # Aktualizacja stanu gry
            await self.update_game_state()
            # Renderowanie
            self.render()
            # Kontrola FPS
            self.clock.tick(60)
            await asyncio.sleep(1/60)

        # Zatrzymaj klienta i zamknij Pygame
        await self.client.left()
        await self.client.stop()
        client_task.cancel()
        pygame.quit()


async def main():
    parser = argparse.ArgumentParser(description="Uruchomienie klienta gry.")
    parser.add_argument("--realm", type=str, default="desert", help="Kraina, do której dołącza gracz (np. 'desert' lub 'forest').")
    args = parser.parse_args()
    
    manager = GameManager(str(uuid.uuid1()))
    await manager.run(args.realm)

if __name__ == "__main__":
    asyncio.run(main())