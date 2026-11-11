FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY src/Orders.Contracts/Orders.Contracts.csproj src/Orders.Contracts/
COPY src/Orders.Data/Orders.Data.csproj src/Orders.Data/
COPY src/Orders.Api/Orders.Api.csproj src/Orders.Api/
RUN dotnet restore src/Orders.Api/Orders.Api.csproj

COPY src/Orders.Contracts/ src/Orders.Contracts/
COPY src/Orders.Data/ src/Orders.Data/
COPY src/Orders.Api/ src/Orders.Api/
RUN dotnet publish src/Orders.Api/Orders.Api.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "Orders.Api.dll"]