FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY src/Orders.Contracts/Orders.Contracts.csproj src/Orders.Contracts/
COPY src/Orders.Data/Orders.Data.csproj src/Orders.Data/
COPY src/Orders.Worker/Orders.Worker.csproj src/Orders.Worker/
RUN dotnet restore src/Orders.Worker/Orders.Worker.csproj

COPY src/Orders.Contracts/ src/Orders.Contracts/
COPY src/Orders.Data/ src/Orders.Data/
COPY src/Orders.Worker/ src/Orders.Worker/
RUN dotnet publish src/Orders.Worker/Orders.Worker.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Orders.Worker.dll"]