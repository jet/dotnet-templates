namespace TodoBackendTemplate;

/// System.Text.Json implementation of IEncoder that encodes direct to a UTF-8 Buffer
public class SystemTextJsonUtf8Codec(TypeShape.UnionContract.IEncoder<ReadOnlyMemory<byte>> codec)
{
    SystemTextJsonUtf8Codec(FsCodec.SystemTextJson.Serdes serdes) : this(new FsCodec.SystemTextJson.Core.ReadOnlyMemoryEncoder(serdes)) { }
    public SystemTextJsonUtf8Codec(System.Text.Json.JsonSerializerOptions options) : this(new FsCodec.SystemTextJson.Serdes(options)) { }
    public ReadOnlyMemory<byte> Encode(object value) => EncodeTyped(value);
    public ReadOnlyMemory<byte> EncodeTyped<T>(T value) => codec.Encode(value);
    public T Decode<T>(ReadOnlyMemory<byte> json) => codec.Decode<T>(json);
}