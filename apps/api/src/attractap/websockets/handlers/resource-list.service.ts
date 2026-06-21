import { Inject, Injectable, Logger } from '@nestjs/common';
import { ResourceFlowNodeType } from '@attraccess/database-entities';
import { WebsocketService } from '../websocket.service';
import { AttractapService } from '../../attractap.service';
import { ResourceUsageService } from '../../../resources/usage/resourceUsage.service';
import { ResourceMaintenanceService } from '../../../resources/maintenances/maintenance.service';
import { ResourceHealthService } from '../../../resources/health/resource-health.service';
import { ResourceFlowsService } from '../../../resources/flows/resource-flows.service';
import { ResourceHealthStatus } from '@attraccess/database-entities';
import { AuthenticatedWebSocket, AttractapEvent, AttractapEventType } from '../websocket.types';

const DEBOUNCE_MS = 200;

@Injectable()
export class ResourceListService {
  private readonly logger = new Logger(ResourceListService.name);

  @Inject(WebsocketService)
  private websocketService: WebsocketService;

  @Inject(AttractapService)
  private attractapService: AttractapService;

  @Inject(ResourceUsageService)
  private resourceUsageService: ResourceUsageService;

  @Inject(ResourceMaintenanceService)
  private resourceMaintenanceService: ResourceMaintenanceService;

  @Inject(ResourceHealthService)
  private resourceHealthService: ResourceHealthService;

  @Inject(ResourceFlowsService)
  private resourceFlowsService: ResourceFlowsService;

  private readonly pendingSends = new Map<number, { timer: ReturnType<typeof setTimeout>; resourceIds: Set<number> }>();

  public async sendResourceList(readerId: number, resourceIds?: Set<number>) {
    const sockets = Array.from(this.websocketService.sockets.values()).filter((socket) => socket.readerId === readerId);
    if (sockets.length === 0) {
      return;
    }

    await Promise.all(
      sockets.map((socket) =>
        resourceIds ? this.sendResourceListToSocket(socket, { resourceIds }) : this.sendResourceListToSocket(socket),
      ),
    );
  }

  public sendResourceListToReadersWithResource(resourceId: number): void {
    const readerIds = new Set<number>();
    for (const socket of this.websocketService.sockets.values()) {
      readerIds.add(socket.readerId);
    }

    for (const readerId of readerIds) {
      this.scheduleSend(readerId, resourceId);
    }
  }

  private scheduleSend(readerId: number, resourceId: number): void {
    const pending = this.pendingSends.get(readerId);
    if (pending) {
      clearTimeout(pending.timer);
      pending.resourceIds.add(resourceId);
    }

    const resourceIds = pending?.resourceIds ?? new Set([resourceId]);

    const timer = setTimeout(() => {
      this.pendingSends.delete(readerId);
      this.sendResourceList(readerId, resourceIds).catch((err) => {
        this.logger.error(`Failed to send debounced resource list to reader ${readerId}`, err);
      });
    }, DEBOUNCE_MS);

    this.pendingSends.set(readerId, { timer, resourceIds });
  }

  public async sendResourceListToSocket(
    socket: AuthenticatedWebSocket,
    onlyIfResourceMatches?: { resourceIds?: Set<number> },
  ) {
    const reader = await this.attractapService.findReaderById(socket.readerId);
    if (!reader) {
      throw new Error(`Reader not found: ${socket.readerId}`);
    }

    const resources = [...reader.resources].sort((a, b) => a.name.localeCompare(b.name));

    if (onlyIfResourceMatches?.resourceIds?.size) {
      if (!resources.some((resource) => onlyIfResourceMatches.resourceIds?.has(resource.id))) {
        return;
      }
    }

    const resourceIds = resources.map((r) => r.id);

    // Fetch all per-resource data in 4 bulk queries instead of 4×N individual queries.
    const [healthMap, activeSessionMap, activeMaintenanceIds, flowButtonMap] = await Promise.all([
      this.resourceHealthService.listForResources(resourceIds),
      this.resourceUsageService.getActiveSessions(resourceIds),
      this.resourceMaintenanceService.getActiveMaintenanceResourceIds(resourceIds),
      this.resourceFlowsService.getNodesForResources(resourceIds, ResourceFlowNodeType.INPUT_BUTTON),
    ]);

    const resourceListResponse = new AttractapEvent(AttractapEventType.RESOURCE_LIST, {
      readerName: reader.name,
      ledBrightness: reader.ledBrightness,
      resources: resources.map((resource) => {
        const healthEntries = healthMap.get(resource.id) ?? [];
        const unhealthyEntries = healthEntries.filter((entry) => entry.status === ResourceHealthStatus.UNHEALTHY);
        const activeUsageSession = activeSessionMap.get(resource.id) ?? null;
        const flowNodes = flowButtonMap.get(resource.id) ?? [];

        return {
          id: resource.id,
          name: resource.name,
          type: resource.type,
          separateUnlockAndUnlatch: resource.separateUnlockAndUnlatch,
          description: resource.description,
          allowTakeOver: resource.allowTakeOver,
          introducers: resource.introducers.map((introducer) => introducer.user.username),
          isUnderMaintenance: activeMaintenanceIds.has(resource.id),
          isHealthy: unhealthyEntries.length === 0,
          healthReason: this.buildHealthReason(unhealthyEntries),
          activeUsageSession: activeUsageSession
            ? {
              user: {
                username: activeUsageSession.user.username,
              },
              startTime: activeUsageSession.startTime.toISOString(),
              // Offset (minutes east of UTC) of the API's effective timezone for this
              // specific instant, so the reader can render local wall-clock time without
              // a tz database. Computed per-timestamp, so it stays DST-correct.
              startTimeUtcOffsetMinutes: -activeUsageSession.startTime.getTimezoneOffset(),
            }
            : null,
          flowButtons: flowNodes.map((node) => ({
            id: node.id,
            label: node.data.label || node.id,
          })),
        };
      }),
    });
    this.logger.debug(`Sending resource list to socket ${socket.id}`, resourceListResponse);
    await socket.sendMessage(resourceListResponse);
  }

  private buildHealthReason(unhealthyEntries: { identifier: string; reason: string | null }[]): string {
    return unhealthyEntries
      .map((entry) => {
        const reason = (entry.reason ?? '').trim() || 'Unhealthy';
        const identifier = (entry.identifier ?? '').trim();
        return identifier ? `${identifier}: ${reason}` : reason;
      })
      .join('\n');
  }
}
