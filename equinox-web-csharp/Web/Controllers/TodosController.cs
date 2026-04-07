
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;

namespace TodoBackendTemplate.Controllers;

// Binds ClientId from the COMPLETELY_INSECURE_CLIENT_ID header, defaulting to Guid.Empty when absent
[AttributeUsage(AttributeTargets.Parameter)]
public class FromClientIdHeaderAttribute() : ModelBinderAttribute(typeof(ClientIdModelBinder))
{
    public override BindingSource BindingSource => BindingSource.Header;
}

sealed class ClientIdModelBinder : IModelBinder
{
    public Task BindModelAsync(ModelBindingContext bindingContext)
    {
        var headerValue = bindingContext.HttpContext.Request.Headers["COMPLETELY_INSECURE_CLIENT_ID"].FirstOrDefault();
        var clientId = string.IsNullOrEmpty(headerValue)
            ? new ClientId(Guid.Empty)
            : new ClientId(Guid.Parse(headerValue));
        bindingContext.Result = ModelBindingResult.Success(clientId);
        return Task.CompletedTask;
    }
}

public class TodoView
{
    public int Id { get; set; }
    public string? Url { get; set; }
    public int Order { get; set; }
    public string? Title { get; set; }
    public bool Completed { get; set; }
}

// Fulfills contract dictated by https://www.todobackend.com
// To run:
//     & dotnet run -p Web
//     https://www.todobackend.com/client/index.html?https://localhost:5001/todos
//     # NB Jet does not own, control or audit https://todobackend.com; it is a third party site; please satisfy yourself that this is a safe thing to use in your environment before using it.
// See also similar backends used as references when implementing:
//     https://github.com/ChristianAlexander/dotnetcore-todo-webapi/blob/master/src/TodoWebApi/Controllers/TodosController.cs
//     https://github.com/joeaudette/playground/blob/master/spa-stack/src/FSharp.WebLib/Controllers.fs
[Route("[controller]"), ApiController]
public class TodosController(Todo.Service service) : ControllerBase
{
    [HttpGet]
    public async Task<IEnumerable<TodoView>> Get([FromClientIdHeader] ClientId clientId) =>
        from x in await service.List(clientId) select WithUri(x);

    [HttpGet("{id}", Name = "GetTodo")]
    public async Task<IActionResult> Get([FromClientIdHeader] ClientId clientId, int id)
    {
        var res = await service.TryGet(clientId, id);
        if (res is null) return NotFound();
        return new ObjectResult(WithUri(res));
    }

    [HttpPost]
    public async Task<TodoView> Post([FromClientIdHeader] ClientId clientId, [FromBody] TodoView value) =>
        WithUri(await service.Create(clientId, ToProps(value)));

    [HttpPatch("{id}")]
    public async Task<TodoView> Patch([FromClientIdHeader] ClientId clientId, int id, [FromBody] TodoView value) =>
        WithUri(await service.Patch(clientId, id, ToProps(value)));

    [HttpDelete("{id}")]
    public Task Delete([FromClientIdHeader] ClientId clientId, int id) =>
        service.Execute(clientId, new Todo.Command.Delete { Id = id });

    [HttpDelete]
    public Task DeleteAll([FromClientIdHeader] ClientId clientId) =>
        service.Execute(clientId, new Todo.Command.Clear());

    static Todo.Props ToProps(TodoView value) =>
        new() { Order = value.Order, Title = value.Title, Completed = value.Completed };

    TodoView WithUri(Todo.View x)
    {
        // Supplying scheme is secret sauce for making it absolute as required by client
        var url = Url.RouteUrl("GetTodo", new { id = x.Id }, Request.Scheme);
        return new TodoView { Id = x.Id, Url = url, Order = x.Order, Title = x.Title, Completed = x.Completed };
    }
}