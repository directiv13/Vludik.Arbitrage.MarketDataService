using MarketDataService.Application.EventHandlers;
using MarketDataService.Application.Services;
using MarketDataService.Application.Validators;
using Microsoft.Extensions.DependencyInjection;

namespace MarketDataService.Application.DependencyInjection;

public static class ApplicationServiceExtensions
{
    /// <summary>
    /// Registers the tick pipeline and the orchestrator. All are stateless singletons —
    /// <see cref="TickReceivedHandler"/> sits on the tick hot path and is captured by the
    /// singleton subscription registry.
    /// </summary>
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        services.AddSingleton<TickValidator>();
        services.AddSingleton<TickReceivedHandler>();
        services.AddSingleton<MarketDataOrchestrator>();

        return services;
    }
}
