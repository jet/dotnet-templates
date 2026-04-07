using System.Runtime.Serialization;

namespace TodoBackendTemplate;

/// ClientId strongly typed id
public class ClientId(Guid value)
{
    [IgnoreDataMember] // Prevent Swashbuckle inferring there is a Value property
    public Guid Value { get; } = value;
    public override string ToString() => Value.ToString("N");
}