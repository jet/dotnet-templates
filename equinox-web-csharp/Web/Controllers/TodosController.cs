using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TodoBackendTemplate.Controllers
{
    public class FromClientIdHeaderAttribute : FromHeaderAttribute
    {
        public FromClientIdHeaderAttribute() =>
            Name = "COMPLETELY_INSECURE_CLIENT_ID";
    }

    public class TodoView
    {
        public int Id { get; set; }
        public string Url { get; set; }
        public int Order { get; set; }
        public string Title { get; set; }
        public bool Completed { get; set; }
    }

    // Fulfills contract dictated by https://www.todobackend.com
    // To run:
    //     & dotnet run -f netcoreapp2.1 -p Web
    //     https://www.todobackend.com/client/index.html?https://localhost:5001/todos
    //     # NB Jet does now own, control or audit https://todobackend.com; it is a third party site; please satisfy yourself that this is a safe thing use in your environment before using it._
    // See also similar backends used as references when implementing:
    //     https://github.com/ChristianAlexander/dotnetcore-todo-webapi/blob/master/src/TodoWebApi/Controllers/TodosController.cs
    //     https://github.com/joeaudette/playground/blob/master/spa-stack/src/FSharp.WebLib/Controllers.fs
    [Route("[controller]"), ApiController]
    public class TodosController : ControllerBase
    {
        readonly Todo.Service _service;

        public TodosController(Todo.Service service) =>
            _service = service;

        [HttpGet]
        public async Task<IEnumerable<TodoView>> Get([FromClientIdHeader] ClientId clientId) =>
            from x in await _service.List(clientId) select WithUri(x);

        [HttpGet("{id}", Name = "GetTodo")]
        public async Task<IActionResult> Get([FromClientIdHeader] ClientId clientId, int id)
        {
            var res = await _service.TryGet(clientId, id);
            if (res == null) return NotFound();
            return new ObjectResult(WithUri(res));
        }

        [HttpPost]
        public async Task<TodoView> Post([FromClientIdHeader] ClientId clientId, [FromBody] TodoView value) =>
            WithUri(await _service.Create(clientId, ToProps(value)));

        [HttpPatch("{id}")]
        public async Task<TodoView> Patch([FromClientIdHeader] ClientId clientId, int id, [FromBody] TodoView value) =>
            WithUri(await _service.Patch(clientId, id, ToProps(value)));

        [HttpDelete("{id}")]
        public Task Delete([FromClientIdHeader] ClientId clientId, int id) =>
            _service.Execute(clientId, new Todo.Commands.Delete {Id = id});

        [HttpDelete]
        public Task DeleteAll([FromClientIdHeader] ClientId clientId) =>
            _service.Execute(clientId, new Todo.Commands.Clear());

        Todo.Props ToProps(TodoView value) =>
            new Todo.Props {Order = value.Order, Title = value.Title, Completed = value.Completed};

        TodoView WithUri(Todo.View x)
        {
            // Supplying scheme is secret sauce for making it absolute as required by client
            var url = Url.RouteUrl("GetTodo", new {id = x.Id}, Request.Scheme);
            return new TodoView {Id = x.Id, Url = url, Order = x.Order, Title = x.Title, Completed = x.Completed};
        }
    }
}