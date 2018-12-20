namespace TodoBackend

open System
open System.Runtime.Serialization

/// Endows any type that inherits this class with standard .NET comparison semantics using a supplied token identifier
[<AbstractClass>]
type Comparable<'TComp, 'Token when 'TComp :> Comparable<'TComp, 'Token> and 'Token : comparison>(token : 'Token) =
    member private __.Token = token // I can haz protected?
    override x.Equals y = match y with :? Comparable<'TComp, 'Token> as y -> x.Token = y.Token | _ -> false
    override __.GetHashCode() = hash token
    interface IComparable with
        member x.CompareTo y =
            match y with
            | :? Comparable<'TComp, 'Token> as y -> compare x.Token y.Token
            | _ -> invalidArg "y" "invalid comparand"

/// ClientId strongly typed id
[<Sealed; AutoSerializable(false); StructuredFormatDisplay("{Value}")>]
// To support model binding using aspnetcore 2 FromHeader
[<System.ComponentModel.TypeConverter(typeof<ClientIdStringConverter>)>]
// (Internally a string for most efficient copying semantics)
type ClientId private (id : string) =
    inherit Comparable<ClientId, string>(id)
    [<IgnoreDataMember>] // Prevent swashbuckle inferring there's a "value" field
    member __.Value = id
    override __.ToString () = id
    // NB tests lean on having a ctor of this shape
    new (guid: Guid) = ClientId (guid.ToString("N"))
    // NB for validation [and XSS] purposes we must prove it translatable to a Guid
    static member Parse(input: string) = ClientId (Guid.Parse input)
and private ClientIdStringConverter() =
    inherit System.ComponentModel.TypeConverter()
    override __.CanConvertFrom(context, sourceType) =
        sourceType = typedefof<string> || base.CanConvertFrom(context,sourceType)
    override __.ConvertFrom(context, culture, value) =
        match value with
        | :? string as s -> s |> ClientId.Parse |> box
        | _ -> base.ConvertFrom(context, culture, value)
    override __.ConvertTo(context, culture, value, destinationType) =
        match value with
        | :? ClientId as value when destinationType = typedefof<string> -> value.Value :> _
        | _ -> base.ConvertTo(context, culture, value, destinationType)