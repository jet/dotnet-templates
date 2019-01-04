# Equinox Web Template

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install in the local templates store

    dotnet new equinoxweb -t --language C# # use --help to see options regarding storage subsystem configuration etc

To run a local instance of the Website on https://localhost:5001 and http://localhost:5000

    dotnet run -p Web

----

**It's strongly recommended to also generate an F# version too - the translation of the template is intentionally not 1:1, and has not recieved as much love and attention as the F# implementation by any stretch and should definitely not be considered the cleanest C# implementation possible. The time investment of looking at both is very likely to pay off both in terms of understanding the goals/patterns of Equinox and troubleshooting**

_Finally: Please raise issues for any and all things that the Todo sample's implementation raises. While event sourcing is pretty debatable for a todo app, and the todobackend mandates a particular implementation, that does not mean it can't be improved. Yes, even by folks like you with Imposter Syndrome!_

----

To exercise the functionality of the sample TodoBackend (included because of the `-t` in the abvoe), you can use the community project https://todobackend.com to drive the backend. _NB Jet does now own, control or audit https://todobackend.com; it is a third party site; please satisfy yourself that this is a safe thing use in your environment before using it._

0. The generated code includes a CORS whitelisting for https://todobackend.com. _Cors configuration should be considered holistically in the overall design of an app - Equinox itself has no requirement of any specific configuration; you should ensure appropriate care and attention is paid to this aspect of securiting your application as normal_.

1. Run the API compliance test suite (can be useful to isolate issues if the application is experiencing internal errors):

    start https://www.todobackend.com/specs/index.html?https://localhost:5001/todos
    
2. Once you've confirmed that the backend is listening and fulfulling the API obligations, you can run the frontend app:

    # Interactive UI; NB error handling is pretty minimal, so hitting refresh and/or F12 is recommended ;)
    start https://www.todobackend.com/client/index.html?https://localhost:5001/todos