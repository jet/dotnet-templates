using System;

namespace TodoBackendTemplate
{
    /// System.Text.Json implementation of IEncoder that encodes direct to a UTF-8 Buffer
    public class SystemTextJsonUtf8Codec
    {
        readonly TypeShape.UnionContract.IEncoder<ReadOnlyMemory<byte>> _codec; 

        public SystemTextJsonUtf8Codec(System.Text.Json.JsonSerializerOptions options) =>
            _codec = new FsCodec.SystemTextJson.Core.ReadOnlyMemoryEncoder(new FsCodec.SystemTextJson.Serdes(options));

        public ReadOnlyMemory<byte> Encode<T>(T value) => _codec.Encode(value);

        public T Decode<T>(ReadOnlyMemory<byte> json) => _codec.Decode<T>(json);
    }
}
