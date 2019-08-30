using System;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.Serialization;

namespace TodoBackendTemplate
{
    /// ClientId strongly typed id
    // To support model binding using aspnetcore 2 FromHeader
    [TypeConverter(typeof(ClientIdStringConverter))]
    public class ClientId
    {
        private ClientId(Guid value) => Value = value;

        [IgnoreDataMember] // Prevent Swashbuckle inferring there is a Value property
        public Guid Value { get; }

        // TOCONSIDER - happy for this to become a ctor and  ClientIdStringConverter to be removed if it just works correctly as-is when a header is supplied
        public static ClientId Parse(string input) => new ClientId(Guid.Parse(input));

        public override string ToString() => Value.ToString("N");
    }

    class ClientIdStringConverter : TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType) =>
            sourceType == typeof(string) || base.CanConvertFrom(context, sourceType);

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value) =>
            value is string s ? ClientId.Parse(s) : base.ConvertFrom(context, culture, value);
    }
}