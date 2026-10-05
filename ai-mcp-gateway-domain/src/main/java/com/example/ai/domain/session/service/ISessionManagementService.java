package com.example.ai.domain.session.service;

import com.example.ai.domain.session.model.valobj.SessionConfigV0;

public interface ISessionManagementService {

    SessionConfigV0 createSession(String gatewayId);

    void removeSession(String sessionId);

    SessionConfigV0 getSession(String sessionId);

    void cleanupExpiredSessions();

    void shutdown();
}
