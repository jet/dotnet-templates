using System;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.Serialization;

namespace TodoBackendTemplate;

/// ClientId strongly typed id
// To support model binding using aspnetcore 2 FromHeader
[TypeConverter(typeof(ClientIdStringConverter))]
public class ClientId
{
    ClientId(Guid value) => Value = value;

    [IgnoreDataMember] // Prevent Swashbuckle inferring there is a Value property
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    public Guid Value { get; }

    // TOCONSIDER - happy for this to become a ctor and ClientIdStringConverter to be removed if it just works correctly as-is
    // when this type is used to Bind to a HTTP Request header
    public static ClientId Parse(string input) => new(Guid.Parse(input));

    public override string ToString() => Value.ToString("N");
}

class ClientIdStringConverter : TypeConverter
{
    public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType) =>
        sourceType == typeof(string) || base.CanConvertFrom(context, sourceType);

    public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value) =>
        value is string s ? ClientId.Parse(s) : base.ConvertFrom(context, culture, value);
}