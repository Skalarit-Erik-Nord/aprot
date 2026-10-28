import { describe, test, expect, afterEach } from 'vitest';
import { wsSmallLimitUrl, wsNoLimitUrl } from './helpers';
import { ApiClient, MessageTooLargeError, type ConnectionState } from '../api/client';
import { setBlob } from '../api/blob-handlers';
import { getConnURL } from '../api/conn-handlers';

// The /ws-small server advertises maxMessageSize 4096 in its config frame. A
// frame larger than that would make the server close the socket, failing
// every call on it, so the client must refuse it locally and fail only the
// call that built it.
const LIMIT = 4096;

// connected opens a client and waits for the server's config frame: it is
// the first frame on the connection, so it has been applied once any
// response arrives.
async function connected(url: string): Promise<{ client: ApiClient; states: ConnectionState[] }> {
    const client = new ApiClient(url, { reconnectInterval: 50, reconnectMaxInterval: 50 });
    const states: ConnectionState[] = [];
    client.onStateChange((s) => states.push(s));
    await client.connect();
    await getConnURL(client);
    return { client, states };
}

async function waitForState(client: ApiClient, state: ConnectionState): Promise<void> {
    const deadline = Date.now() + 2000;
    while (client.getState() !== state) {
        if (Date.now() > deadline) throw new Error(`state stayed ${client.getState()}, want ${state}`);
        await new Promise((r) => setTimeout(r, 5));
    }
}

describe('maxMessageSize from the config frame', () => {
    let client: ApiClient | undefined;

    afterEach(() => {
        client?.disconnect();
        client = undefined;
    });

    test('an oversized request fails locally and the connection stays up', async () => {
        const c = await connected(wsSmallLimitUrl());
        client = c.client;

        const err = await setBlob(client, 'x'.repeat(LIMIT * 2)).catch((e: unknown) => e);
        expect(err).toBeInstanceOf(MessageTooLargeError);
        const tooLarge = err as MessageTooLargeError;
        expect(tooLarge.limit).toBe(LIMIT);
        expect(tooLarge.size).toBeGreaterThan(LIMIT);
        expect(tooLarge.message).toContain(`${tooLarge.size} bytes`);
        expect(tooLarge.message).toContain(`${LIMIT} bytes`);

        // The socket was never touched: no state change, and calls that fit
        // keep working on the same connection.
        expect(client.getState()).toBe('connected');
        await expect(setBlob(client, 'fits')).resolves.toBeNull();
        await expect(getConnURL(client)).resolves.toBeDefined();
        expect(c.states).toEqual(['connecting', 'connected']);
    });

    test('the limit counts UTF-8 bytes, not string length', async () => {
        client = (await connected(wsSmallLimitUrl())).client;

        // 3000 characters but 6000 bytes of UTF-8.
        const err = await setBlob(client, 'é'.repeat(3000)).catch((e: unknown) => e);
        expect(err).toBeInstanceOf(MessageTooLargeError);
        expect((err as MessageTooLargeError).size).toBeGreaterThan(6000);

        // 3000 single-byte characters fit.
        await expect(setBlob(client, 'e'.repeat(3000))).resolves.toBeNull();
    });

    test('an oversized subscription fails through onError and is dropped', async () => {
        const c = await connected(wsSmallLimitUrl());
        client = c.client;

        const errors: Error[] = [];
        const results: unknown[] = [];
        client.subscribe('BlobHandlers.GetBlob', ['x'.repeat(LIMIT * 2)], (r) => results.push(r), (e) => errors.push(e));
        await new Promise((r) => setTimeout(r, 50));

        expect(errors).toHaveLength(1);
        expect(errors[0]).toBeInstanceOf(MessageTooLargeError);
        expect(results).toHaveLength(0);
        expect(client.getState()).toBe('connected');
        expect(client.getLoadingCount()).toBe(0);
        await expect(getConnURL(client)).resolves.toBeDefined();
        expect(c.states).toEqual(['connecting', 'connected']);
    });

    test('an oversized stream fails locally', async () => {
        client = (await connected(wsSmallLimitUrl())).client;

        const iterate = async () => {
            for await (const _ of client!.requestStream('StreamingHandlers.Numbers', ['x'.repeat(LIMIT * 2)])) {
                // Never reached: the stream fails before it is sent.
            }
        };
        await expect(iterate()).rejects.toBeInstanceOf(MessageTooLargeError);
        expect(client.getState()).toBe('connected');
        await expect(getConnURL(client)).resolves.toBeDefined();
    });

    test('a request queued during a reconnect is checked when it is flushed', async () => {
        client = (await connected(wsSmallLimitUrl())).client;

        // Drop the socket without disconnect(), so the client reconnects.
        (client as unknown as { transport: { disconnect: () => void } }).transport.disconnect();
        await waitForState(client, 'reconnecting');

        const oversized = setBlob(client, 'x'.repeat(LIMIT * 2)).catch((e: unknown) => e);
        const fits = setBlob(client, 'fits');

        // Both are flushed as the socket opens, before the new connection's
        // config frame arrives, so the last advertised limit applies.
        expect(await oversized).toBeInstanceOf(MessageTooLargeError);
        await expect(fits).resolves.toBeNull();
        expect(client.getState()).toBe('connected');
    });

    test('with no advertised limit nothing is rejected locally', async () => {
        client = (await connected(wsNoLimitUrl())).client;

        await expect(setBlob(client, 'x'.repeat(256 * 1024))).resolves.toBeNull();
        expect(client.getState()).toBe('connected');
    });
});
