using System;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

class WebSocketChatClient
{
    private ClientWebSocket _webSocket;
    private readonly string _clientId;
    private readonly string _serverUrl;
    private readonly CancellationTokenSource _cts;
    private bool _isConnected;

    public WebSocketChatClient(string serverUrl)
    {
        _webSocket = new ClientWebSocket();
        _clientId = Guid.NewGuid().ToString("N")[..8];
        _serverUrl = serverUrl;
        _cts = new CancellationTokenSource();
        _isConnected = false;
    }

    public async Task ConnectAsync()
    {
        try
        {
            Console.WriteLine($"[{_clientId}] Підключення до сервера...");
            await _webSocket.ConnectAsync(new Uri(_serverUrl), _cts.Token);
            _isConnected = true;
            Console.WriteLine($"[{_clientId}] Підключено! Ви можете почати спілкування.");
            Console.WriteLine("Команди: /quit - вийти, /help - допомога\n");
            
            _ = Task.Run(ReceiveMessagesAsync);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Помилка підключення: {ex.Message}");
            throw;
        }
    }

    private async Task ReceiveMessagesAsync()
    {
        var buffer = new byte[4096];
        
        while (_webSocket.State == WebSocketState.Open && !_cts.IsCancellationRequested)
        {
            try
            {
                var result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), _cts.Token);
                
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", _cts.Token);
                    _isConnected = false;
                    Console.WriteLine("\nЗ'єднання з сервером закрито");
                    break;
                }
                
                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var messageJson = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    var serverMsg = JsonSerializer.Deserialize<ServerMessage>(messageJson);
                    
                    if (serverMsg?.Type == "history" && serverMsg.Messages != null)
                    {
                        Console.WriteLine("\n=== Історія повідомлень ===");
                        foreach (var msg in serverMsg.Messages)
                        {
                            PrintMessage(msg);
                        }
                        Console.WriteLine("=== Поточний чат ===\n");
                    }
                    else if (serverMsg?.Type == "message" && serverMsg.Message != null)
                    {
                        PrintMessage(serverMsg.Message);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                if (_isConnected)
                    Console.WriteLine($"\nПомилка отримання повідомлення: {ex.Message}");
                break;
            }
        }
    }

    private void PrintMessage(ChatMessage msg)
    {
        var timestamp = msg.Timestamp.ToLocalTime().ToString("HH:mm:ss");
        if (msg.IsSystem)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"[{timestamp}] {msg.Text}");
        }
        else
        {
            Console.ForegroundColor = msg.From == _clientId ? ConsoleColor.Green : ConsoleColor.Cyan;
            Console.WriteLine($"[{timestamp}] <{msg.From}>: {msg.Text}");
        }
        Console.ResetColor();
    }

    public async Task SendMessageAsync(string text)
    {
        if (_webSocket.State != WebSocketState.Open)
        {
            Console.WriteLine("Не підключено до сервера");
            return;
        }
        
        var message = new ClientMessage { Text = text };
        var json = JsonSerializer.Serialize(message);
        var bytes = Encoding.UTF8.GetBytes(json);
        
        try
        {
            await _webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, _cts.Token);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Помилка відправки: {ex.Message}");
        }
    }

    public async Task DisconnectAsync()
    {
        if (_webSocket.State == WebSocketState.Open)
        {
            try
            {
                await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Client quit", _cts.Token);
            }
            catch { }
        }
        _cts.Cancel();
        _webSocket.Dispose();
        _isConnected = false;
    }

    public void ShowHelp()
    {
        Console.WriteLine("\n=== Допомога ===");
        Console.WriteLine("/quit - вийти з чату");
        Console.WriteLine("/help - показати цю довідку");
        Console.WriteLine("Будь-який інший текст буде відправлено як повідомлення\n");
    }

    public bool IsConnected => _isConnected;
    public string ClientId => _clientId;
}

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== WebSocket Chat Client ===\n");
        Console.Write("Введіть URL сервера (наприклад, ws://localhost:5000/chat): ");
        string serverUrl = Console.ReadLine()?.Trim();
        
        if (string.IsNullOrEmpty(serverUrl))
        {
            serverUrl = "ws://localhost:5000/chat";
            Console.WriteLine($"Використовується URL за замовчуванням: {serverUrl}");
        }
        
        var client = new WebSocketChatClient(serverUrl);
        
        try
        {
            await client.ConnectAsync();
            
            Console.WriteLine("Введіть /help для списку команд\n");
            
            while (client.IsConnected)
            {
                var input = Console.ReadLine();
                
                if (string.IsNullOrEmpty(input))
                    continue;
                
                if (input == "/quit")
                {
                    Console.WriteLine("Вихід...");
                    break;
                }
                else if (input == "/help")
                {
                    client.ShowHelp();
                }
                else
                {
                    await client.SendMessageAsync(input);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Критична помилка: {ex.Message}");
        }
        finally
        {
            await client.DisconnectAsync();
            Console.WriteLine("З'єднання закрито. Натисніть будь-яку клавішу для виходу...");
            Console.ReadKey();
        }
    }
}

class ChatMessage
{
    public string From { get; set; } = "";
    public string Text { get; set; } = "";
    public bool IsSystem { get; set; }
    public DateTime Timestamp { get; set; }
}

class ClientMessage
{
    public string? Text { get; set; }
}

class ServerMessage
{
    public string Type { get; set; } = "";
    public List<ChatMessage>? Messages { get; set; }
    public ChatMessage? Message { get; set; }
}
