using System;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var clients = new ConcurrentDictionary<string, WebSocket>();
var messageHistory = new List<ChatMessage>();
var historyLock = new object();

app.UseWebSockets();

app.Map("/chat", async context =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        var clientId = Guid.NewGuid().ToString("N")[..8];
        
        clients[clientId] = webSocket;
        Console.WriteLine($"[+] Connected: {clientId}, Total: {clients.Count}");
        
        // Відправляємо історію новому клієнту
        var historyMsg = new ServerMessage 
        { 
            Type = "history", 
            Messages = GetMessageHistory() 
        };
        await SendToClient(webSocket, historyMsg);
        
        // Повідомляємо всім про нового клієнта
        await BroadcastMessage(new ChatMessage
        {
            From = "server",
            Text = $"Client {clientId} joined",
            IsSystem = true,
            Timestamp = DateTime.UtcNow
        }, clientId);
        
        var buffer = new byte[4096];
        
        try
        {
            while (webSocket.State == WebSocketState.Open)
            {
                var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                
                if (result.MessageType == WebSocketMessageType.Close)
                    break;
                
                if (result.MessageType == WebSocketMessageType.Text && result.Count > 0)
                {
                    var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    Console.WriteLine($"[RECV] {clientId}: {json}");
                    
                    try
                    {
                        var clientMsg = JsonSerializer.Deserialize<ClientMessage>(json);
                        if (!string.IsNullOrWhiteSpace(clientMsg?.Text))
                        {
                            var chatMsg = new ChatMessage
                            {
                                From = clientId,
                                Text = clientMsg.Text,
                                IsSystem = false,
                                Timestamp = DateTime.UtcNow
                            };
                            
                            // Зберігаємо в історію
                            lock (historyLock)
                            {
                                messageHistory.Add(chatMsg);
                                if (messageHistory.Count > 100)
                                    messageHistory.RemoveAt(0);
                            }
                            
                            // Розсилаємо ВСІМ клієнтам (включаючи відправника?)
                            Console.WriteLine($"[BROADCAST] {clientId}: {clientMsg.Text}");
                            await BroadcastMessage(chatMsg);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Parse error: {ex.Message}");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] Client {clientId}: {ex.Message}");
        }
        finally
        {
            clients.TryRemove(clientId, out _);
            Console.WriteLine($"[-] Disconnected: {clientId}, Total: {clients.Count}");
            
            await BroadcastMessage(new ChatMessage
            {
                From = "server",
                Text = $"Client {clientId} left",
                IsSystem = true,
                Timestamp = DateTime.UtcNow
            });
            
            try
            {
                if (webSocket.State != WebSocketState.Closed && webSocket.State != WebSocketState.Aborted)
                    await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None);
            }
            catch { }
            webSocket.Dispose();
        }
    }
    else
    {
        context.Response.StatusCode = 400;
        await context.Response.WriteAsync("WebSocket required");
    }
});

async Task BroadcastMessage(ChatMessage message, string excludeClientId = null)
{
    var serverMsg = new ServerMessage { Type = "message", Message = message };
    var json = JsonSerializer.Serialize(serverMsg);
    var bytes = Encoding.UTF8.GetBytes(json);
    var segment = new ArraySegment<byte>(bytes);
    
    foreach (var client in clients)
    {
        // Відправляємо всім, включаючи відправника (щоб він бачив своє повідомлення)
        if (client.Value.State == WebSocketState.Open && (excludeClientId == null || client.Key != excludeClientId))
        {
            try
            {
                await client.Value.SendAsync(segment, WebSocketMessageType.Text, true, CancellationToken.None);
                Console.WriteLine($"[SENT] to {client.Key}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Send to {client.Key} failed: {ex.Message}");
            }
        }
    }
}

async Task SendToClient(WebSocket socket, ServerMessage message)
{
    try
    {
        var json = JsonSerializer.Serialize(message);
        var bytes = Encoding.UTF8.GetBytes(json);
        await socket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR] Send failed: {ex.Message}");
    }
}

List<ChatMessage> GetMessageHistory()
{
    lock (historyLock)
    {
        return new List<ChatMessage>(messageHistory);
    }
}

string port = Environment.GetEnvironmentVariable("PORT") ?? "5000";
app.Run($"http://0.0.0.0:{port}");

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
