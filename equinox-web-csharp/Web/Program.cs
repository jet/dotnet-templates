using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Serilog.Events;
using System;

namespace TodoBackendTemplate.Web
{
    static class Program
    {
        public static async int Main(string[] argv)
        {
            try
            {
                Log.Logger = new LoggerConfiguration()
                    .MinimumLevel.Debug()
                    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                    .Enrich.FromLogContext()
                    .WriteTo.Console()
                    .CreateLogger();
                var host = WebHost
                    .CreateDefaultBuilder(argv)
                    .UseStartup<Startup>()
                    .Build();
                // Conceptually, these can run in parallel
                // in practice, you'll only very rarely have >1 store
                foreach (var ctx in host.Services.GetServices<EquinoxContext>())
                    await ctx.Connect();
                host.Run();
                return 0;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                return 1;
            }
        }
    }
}