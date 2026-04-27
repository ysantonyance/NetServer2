FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /app

# Копіюємо файли проекту
COPY NetServer.csproj ./
RUN dotnet restore

# Копіюємо всі файли та будуємо
COPY . ./
RUN dotnet publish NetServer.csproj -c Release -o out

# Фінальний образ
FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app/out .

# Встановлюємо порт
ENV PORT=5000
EXPOSE 5000

# Запускаємо сервер
ENTRYPOINT ["dotnet", "NetServer.dll"]
