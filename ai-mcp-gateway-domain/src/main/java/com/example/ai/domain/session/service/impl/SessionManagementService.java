package com.example.ai.domain.session.service.impl;

import com.example.ai.domain.session.model.valobj.SessionConfigV0;
import com.example.ai.domain.session.service.ISessionManagementService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@Service
public class SessionManagementService implements ISessionManagementService {

    private static final long SESSION_TIMEOUT_MINUTES = 30;

    private final ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor();

    private final Map<String,SessionConfigV0> activeSessions = new ConcurrentHashMap<>();

    public SessionManagementService(){
        cleanupScheduler.scheduleAtFixedRate(this::cleanupExpiredSessions,5,5,TimeUnit.MINUTES);
    }

    @Override
    public SessionConfigV0 createSession(String gatewayId) {

        String sessionId = UUID.randomUUID().toString();

        Sinks.Many<ServerSentEvent<String>> sink = Sinks.many().multicast().onBackpressureBuffer();

        String messageEndpoint = '/' + gatewayId + "/mcp/message?sessionId=" + sessionId;
        sink.tryEmitNext(ServerSentEvent.<String>builder()
                .event("endpoint")
                .data(messageEndpoint)
                .build()
        );
        SessionConfigV0 sessionConfigV0 = new SessionConfigV0(sessionId,sink);

        activeSessions.put(sessionId,sessionConfigV0);

        log.info("创建会话 gatewayId:{}",gatewayId);

        return sessionConfigV0;
    }

    @Override
    public void removeSession(String sessionId) {
        SessionConfigV0 sessionConfigV0 = activeSessions.remove(sessionId);
        if(null == sessionId) return;

        sessionConfigV0.markInactive();

        try{
            sessionConfigV0.getSink().tryEmitComplete();
        }catch (Exception e){
            log.warn("关闭会话失败",e);
        }
    }

    @Override
    public SessionConfigV0 getSession(String sessionId) {
        if(null == sessionId || sessionId.isEmpty()){
            return null;
        }
        SessionConfigV0 sessionConfigV0 = activeSessions.get(sessionId);

        if (null != sessionConfigV0 && sessionConfigV0.isActive()){
            sessionConfigV0.updateLastAccessed();
            return sessionConfigV0;
        }

        return null;
    }

    @Override
    public void cleanupExpiredSessions() {
        int cleanedCount = 0;

        for (Map.Entry<String,SessionConfigV0> entry : activeSessions.entrySet()){
            SessionConfigV0 sessionConfigV0 = entry.getValue();

            if(!sessionConfigV0.isActive() || sessionConfigV0.isExpired(SESSION_TIMEOUT_MINUTES)){
                removeSession(sessionConfigV0.getSessionId());
                cleanedCount++;
            }
        }
        if(cleanedCount>0){
            log.info("清理了{}",cleanedCount);
        }
    }

    @Override
    public void shutdown() {
        for (String sessionId : activeSessions.keySet()){
            removeSession(sessionId);
        }
        cleanupScheduler.shutdown();
        try{
                if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)){
                    cleanupScheduler.shutdown();
                }
        }catch (InterruptedException e){
            cleanupScheduler.shutdown();
            Thread.currentThread().interrupt();

        }
    }
}
