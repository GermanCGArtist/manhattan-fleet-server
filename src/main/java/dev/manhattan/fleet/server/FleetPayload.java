package dev.manhattan.fleet.server;

import dev.manhattan.fleet.wire.FleetWire;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.Identifier;

/**
 * Single versioned JSON {@link CustomPacketPayload} channel ({@code manhattan_fleet:msg})
 * shared between {@code manhattan-fleet-server} and Manhattan clients (see {@link FleetWire}).
 */
public record FleetPayload(String json) implements CustomPacketPayload {
    public static final Identifier CHANNEL_ID =
            Identifier.fromNamespaceAndPath(FleetWire.CHANNEL_NAMESPACE, FleetWire.CHANNEL_PATH);
    public static final Type<FleetPayload> TYPE = new Type<>(CHANNEL_ID);

    public static final int MAX_JSON_BYTES = FleetWire.MAX_JSON_BYTES;

    public static final StreamCodec<FriendlyByteBuf, FleetPayload> CODEC = StreamCodec.of(
            (buf, payload) -> buf.writeUtf(payload.json() == null ? "{}" : payload.json(), MAX_JSON_BYTES),
            buf -> new FleetPayload(buf.readUtf(MAX_JSON_BYTES))
    );

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}
