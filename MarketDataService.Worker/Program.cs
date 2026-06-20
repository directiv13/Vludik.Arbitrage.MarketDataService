using MarketDataService.Application.DependencyInjection;
using MarketDataService.Infrastructure.DependencyInjection;
using MarketDataService.Worker;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

// Structured console logging via Serilog, configured from appsettings.
builder.Services.AddSerilog((services, configuration) => configuration
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services)
    .Enrich.FromLogContext()
    .WriteTo.Console());

builder.Services
    .AddApplication()
    .AddInfrastructure(builder.Configuration);

builder.Services.AddHostedService<MarketDataWorker>();

var host = builder.Build();
host.Run();
