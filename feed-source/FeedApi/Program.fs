namespace FeedApiTemplate

open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Hosting

module Program =

    let createHostBuilder args =
        Host.CreateDefaultBuilder(args)
            .ConfigureWebHostDefaults(fun webBuilder ->
                webBuilder.UseStartup<Startup>() |> ignore
            )

    [<EntryPoint>]
    let main args =
        createHostBuilder(args).Build().Run()

        let exitCode = 0 in exitCode
