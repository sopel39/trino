/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.FullConnectorSession;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.DispatchQuery;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;
import io.trino.spi.security.Identity;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.security.AccessControlUtil.checkCanKillQueryOwnedBy;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class KillUserQueriesProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle KILL_USER_QUERIES = methodHandle(KillUserQueriesProcedure.class, "killUserQueries", String.class, String.class, ConnectorSession.class);

    private final Optional<DispatchManager> dispatchManager;
    private final AccessControl accessControl;

    @Inject
    public KillUserQueriesProcedure(Optional<DispatchManager> dispatchManager, AccessControl accessControl)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @UsedByGeneratedCode
    public void killUserQueries(String user, String message, ConnectorSession session)
    {
        checkProcedureArgument(user != null, "user cannot be null");
        checkState(dispatchManager.isPresent(), "No dispatch manager is set. kill_user_queries procedure should be executed on coordinator.");

        List<DispatchQuery> queries = dispatchManager.get().getQueryTracker().getAllQueries().stream()
                .filter(query -> user.equals(query.getSession().getUser()) && !query.isDone())
                .collect(toImmutableList());

        if (queries.isEmpty()) {
            throw new TrinoException(NOT_FOUND, "No queries found for user: " + user);
        }

        Identity callerIdentity = ((FullConnectorSession) session).getSession().getIdentity();
        for (DispatchQuery query : queries) {
            checkCanKillQueryOwnedBy(callerIdentity, query.getSession().getIdentity(), accessControl);
            query.fail(KillQueryProcedure.createKillQueryException(message));
        }
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "runtime",
                "kill_user_queries",
                ImmutableList.<Argument>builder()
                        .add(new Argument("USER", VARCHAR))
                        .add(new Argument("MESSAGE", VARCHAR, false, null))
                        .build(),
                KILL_USER_QUERIES.bindTo(this));
    }
}
