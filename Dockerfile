FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}
WORKDIR /src
COPY . .
RUN dotnet restore MarketDataService.Worker/MarketDataService.Worker.csproj
RUN dotnet publish MarketDataService.Worker/MarketDataService.Worker.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:9.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "MarketDataService.Worker.dll"]
