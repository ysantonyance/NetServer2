using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var clients = new List<WebSocket>();

app.UseWebSockets();

app.Map("/chat", async context =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        var socket = await context.WebSockets.AcceptWebSocketAsync();
        clients.Add(socket);
        Console.WriteLine($"Client connected. Total: {clients.Count}");
        
        var buffer = new byte[4096];
        
        try
        {
            while (socket.State == WebSocketState.Open)
            {
                var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                
                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var text = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    Console.WriteLine($"Received: {text}");
                    
                    // Відправляємо всім клієнтам
                    var broadcast = Encoding.UTF8.GetBytes(text);
                    foreach (var client in clients)
                    {
                        if (client.State == WebSocketState.Open)
                        {
                            try
                            {
                                await client.SendAsync(new ArraySegment<byte>(broadcast), WebSocketMessageType.Text, true, CancellationToken.None);
                            }
                            catch { }
                        }
                    }
                }
                else if (result.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }
            }
        }
        finally
        {
            clients.Remove(socket);
            Console.WriteLine($"Client disconnected. Total: {clients.Count}");
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed", CancellationToken.None);
            socket.Dispose();
        }
    }
    else
    {
        context.Response.StatusCode = 400;
    }
});

string port = Environment.GetEnvironmentVariable("PORT") ?? "5000";
app.Run($"http://0.0.0.0:{port}");
