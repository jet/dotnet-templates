namespace TodoBackendTemplate.Controllers

open Microsoft.AspNetCore.Mvc
open TodoBackendTemplate

type FromClientIdHeaderAttribute() = inherit FromHeaderAttribute(Name="COMPLETELY_INSECURE_CLIENT_ID")

type TodoView =
    {   id: int
        url: string
        order: int; title: string; completed: bool }

type GetByIdArgsTemplate = { id: int }

// Fulfills contract dictated by https://www.todobackend.com
// To run:
//     & dotnet run -f netcoreapp2.1 -p Web
//     https://www.todobackend.com/client/index.html?https://localhost:5001/todos
//     # NB Jet does now own, control or audit https://todobackend.com; it is a third party site; please satisfy yourself that this is a safe thing use in your environment before using it._
// See also similar backends used as references when implementing:
//     https://github.com/ChristianAlexander/dotnetcore-todo-webapi/blob/master/src/TodoWebApi/Controllers/TodosController.cs
//     https://github.com/joeaudette/playground/blob/master/spa-stack/src/FSharp.WebLib/Controllers.fs
[<Route "[controller]"; ApiController>]
type TodosController(service: Todo.Service) =
    inherit ControllerBase()

    let toProps (value : TodoView) : Todo.Props = { order = value.order; title = value.title; completed = value.completed }

    member private __.WithUri(x : Todo.View) : TodoView =
        let url = __.Url.RouteUrl("GetTodo", { id=x.id }, __.Request.Scheme) // Supplying scheme is secret sauce for making it absolute as required by client
        { id = x.id; url = url; order = x.order; title = x.title; completed = x.completed }

    [<HttpGet>]
    member __.Get([<FromClientIdHeader>]clientId : ClientId) = async {
        let! xs = service.List(clientId)
        return seq { for x in xs -> __.WithUri(x) }
    }

    [<HttpGet("{id}", Name="GetTodo")>]
    member __.Get([<FromClientIdHeader>]clientId : ClientId, id) : Async<IActionResult> = async {
        let! x = service.TryGet(clientId, id)
        return match x with None -> __.NotFound() :> _ | Some x -> ObjectResult(__.WithUri x) :> _
    }

    [<HttpPost>]
    member __.Post([<FromClientIdHeader>]clientId : ClientId, [<FromBody>]value : TodoView) : Async<TodoView> = async {
        let! created = service.Create(clientId, toProps value)
        return __.WithUri created
    }

    [<HttpPatch "{id}">]
    member __.Patch([<FromClientIdHeader>]clientId : ClientId, id, [<FromBody>]value : TodoView) : Async<TodoView> = async {
        let! updated = service.Patch(clientId, id, toProps value)
        return __.WithUri updated
    }

    [<HttpDelete "{id}">]
    member __.Delete([<FromClientIdHeader>]clientId : ClientId, id): Async<unit> =
        service.Execute(clientId, Todo.Commands.Delete id)

    [<HttpDelete>]
    member __.DeleteAll([<FromClientIdHeader>]clientId : ClientId): Async<unit> =
        service.Execute(clientId, Todo.Commands.Clear)