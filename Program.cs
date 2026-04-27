using System;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// Сховище підключених клієнтів
var _clients = new ConcurrentDictionary<string, ClientConnection>();
var _messages = new List<ChatMessage>();
var _messagesLock = new object();

// Middleware для WebSocket
app.UseWebSockets();

app.MapGet("/", async context =>
{
    context.Response.ContentType = "text/html";
    string html = "<!DOCTYPE html><html><head><title>WebSocket Chat Server</title></head><body><h1>WebSocket Chat Server Running</h1><p>WebSocket endpoint: ws://" + context.Request.Host + "/chat</p></body></html>";
    await context.Response.WriteAsync(html);
});

app.Map("/chat", async context =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        using var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        string clientId = Guid.NewGuid().ToString("N")[..8];
        
        // Реєстрація нового клієнта
        var client = new ClientConnection
        {
            Id = clientId,
            Socket = webSocket,
            LastSeen = DateTime.UtcNow
        };
        
        _clients[clientId] = client;
        
        string joinMsg = $"Клієнт {clientId} приєднався до чату";
        AddSystemMessage(joinMsg);
        Console.WriteLine($"[+] {joinMsg}. Клієнтів: {_clients.Count}");
        
        // Відправляємо історію повідомлень новому клієнту
        await SendToClient(client, new ServerMessage
        {
            Type = "history",
            Messages = GetMessagesCopy()
        });
        
        // Повідомляємо всім про нового користувача
        await BroadcastSystemMessage(joinMsg);
        
        // Обробка вхідних повідомлень
        var buffer = new byte[4096];
        try
        {
            while (webSocket.State == WebSocketState.Open)
            {
                var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None);
                    break;
                }
                
                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var messageText = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    var clientMessage = JsonSerializer.Deserialize<ClientMessage>(messageText);
                    
                    if (clientMessage?.Text != null)
                    {
                        AddMessage(clientId, clientMessage.Text);
                        Console.WriteLine($"[{clientId}]: {clientMessage.Text}");
                        
                        // Розсилаємо повідомлення всім клієнтам
                        await BroadcastMessage(new ServerMessage
                        {
                            Type = "message",
                            Message = new ChatMessage
                            {
                                From = clientId,
                                Text = clientMessage.Text,
                                IsSystem = false,
                                Timestamp = DateTime.UtcNow
                            }
                        });
                    }
                }
                
                client.LastSeen = DateTime.UtcNow;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Помилка з клієнтом {clientId}: {ex.Message}");
        }
        finally
        {
            // Видалення клієнта
            _clients.TryRemove(clientId, out _);
            string leaveMsg = $"Клієнт {clientId} покинув чат";
            AddSystemMessage(leaveMsg);
            Console.WriteLine($"[-] {leaveMsg}. Клієнтів: {_clients.Count}");
            await BroadcastSystemMessage(leaveMsg);
            
            if (webSocket.State != WebSocketState.Closed)
                await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None);
        }
    }
    else
    {
        context.Response.StatusCode = 400;
        await context.Response.WriteAsync("WebSocket connection expected");
    }
});

// Фоновий таймер для очищення неактивних клієнтів
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(TimeSpan.FromSeconds(30));
        var cutoff = DateTime.UtcNow.AddSeconds(-120);
        foreach (var (id, client) in _clients)
        {
            if (client.LastSeen < cutoff && _clients.TryRemove(id, out _))
            {
                string timeoutMsg = $"Клієнт {id} відключився (таймаут)";
                AddSystemMessage(timeoutMsg);
                Console.WriteLine($"[~] {timeoutMsg}. Клієнтів: {_clients.Count}");
                
                try
                {
                    if (client.Socket.State == WebSocketState.Open)
                    {
                        await client.Socket.CloseAsync(WebSocketCloseStatus.EndpointUnavailable, 
                            "Timeout", CancellationToken.None);
                    }
                }
                catch { }
            }
        }
    }
});

void AddMessage(string clientId, string text)
{
    lock (_messagesLock)
    {
        _messages.Add(new ChatMessage 
        { 
            From = clientId, 
            Text = text, 
            IsSystem = false,
            Timestamp = DateTime.UtcNow
        });
        
        // Обмежуємо історію 1000 повідомленнями
        while (_messages.Count > 1000)
            _messages.RemoveAt(0);
    }
}

void AddSystemMessage(string text)
{
    lock (_messagesLock)
    {
        _messages.Add(new ChatMessage 
        { 
            From = "server", 
            Text = text, 
            IsSystem = true,
            Timestamp = DateTime.UtcNow
        });
    }
}

List<ChatMessage> GetMessagesCopy()
{
    lock (_messagesLock)
    {
        return new List<ChatMessage>(_messages);
    }
}

async Task BroadcastMessage(ServerMessage message)
{
    var json = JsonSerializer.Serialize(message);
    var bytes = Encoding.UTF8.GetBytes(json);
    var segment = new ArraySegment<byte>(bytes);
    
    var tasks = new List<Task>();
    foreach (var client in _clients.Values)
    {
        if (client.Socket.State == WebSocketState.Open)
        {
            tasks.Add(client.Socket.SendAsync(segment, WebSocketMessageType.Text, true, CancellationToken.None));
        }
    }
    
    if (tasks.Count > 0)
        await Task.WhenAll(tasks);
}

async Task BroadcastSystemMessage(string text)
{
    await BroadcastMessage(new ServerMessage
    {
        Type = "message",
        Message = new ChatMessage
        {
            From = "server",
            Text = text,
            IsSystem = true,
            Timestamp = DateTime.UtcNow
        }
    });
}

async Task SendToClient(ClientConnection client, ServerMessage message)
{
    try
    {
        var json = JsonSerializer.Serialize(message);
        var bytes = Encoding.UTF8.GetBytes(json);
        var segment = new ArraySegment<byte>(bytes);
        
        await client.Socket.SendAsync(segment, WebSocketMessageType.Text, true, CancellationToken.None);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Помилка відправки клієнту {client.Id}: {ex.Message}");
    }
}

string port = Environment.GetEnvironmentVariable("PORT") ?? "5000";
app.Run($"http://0.0.0.0:{port}");

class ClientConnection
{
    public required string Id { get; set; }
    public required WebSocket Socket { get; set; }
    public DateTime LastSeen { get; set; }
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
