package dev.manhattan.fleet.server;

import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.Identifier;

/**
 * Single versioned JSON {@link CustomPacketPayload} channel ({@code manhattan_fleet:msg})
 * shared between {@code manhattan-fleet-server} and Manhattan clients.
 */
public record FleetPayload(String json) implements CustomPacketPayload {
    public static final Identifier CHANNEL_ID = Identifier.fromNamespaceAndPath("manhattan_fleet", "msg");
    public static final Type<FleetPayload> TYPE = new Type<>(CHANNEL_ID);

    /** Allow up to 512 KiB JSON payloads (for Project definitions with large area/task lists). */
    public static final int MAX_JSON_BYTES = 512 * 1024;

    public static final StreamCodec<FriendlyByteBuf, FleetPayload> CODEC = StreamCodec.of(
            (buf, payload) -> buf.writeUtf(payload.json() == null ? "{}" : payload.json(), MAX_JSON_BYTES),
            buf -> new FleetPayload(buf.readUtf(MAX_JSON_BYTES))
    );

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}
