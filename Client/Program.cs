using System.Net.WebSockets;
using System.Text;

Console.OutputEncoding = Encoding.UTF8;
Console.Title = "CHAT";
Console.WriteLine("=== Чат клієнт ===\n");

Console.Write("Введіть URL сервера (Enter - wss://netserver2.onrender.com/chat): ");
string url = Console.ReadLine()?.Trim();
if (string.IsNullOrEmpty(url)) url = "wss://netserver2.onrender.com/chat";

using var ws = new ClientWebSocket();
await ws.ConnectAsync(new Uri(url), CancellationToken.None);
Console.WriteLine("Підключено до чату!");
Console.WriteLine("Пишіть повідомлення. /quit - вихід\n");

// Потік для отримання повідомлень
_ = Task.Run(async () =>
{
    var buffer = new byte[4096];
    while (ws.State == WebSocketState.Open)
    {
        try
        {
            var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            var msg = Encoding.UTF8.GetString(buffer, 0, result.Count);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"\n{msg}");
            Console.ResetColor();
            Console.Write("> ");
        }
        catch { }
    }
});

// Відправка повідомлень
while (true)
{
    Console.Write("> ");
    var text = Console.ReadLine();
    if (text == "/quit") break;

    if (!string.IsNullOrWhiteSpace(text))
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        await ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
    }
}

await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Bye", CancellationToken.None);
