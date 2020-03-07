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
package io.prestosql.sql.planner.optimizations.joins;

import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NullLiteral;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static java.util.Objects.requireNonNull;

public class JoinNormalizer
{
    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;
    private final Session session;
    private final SymbolAllocator symbolAllocator;

    public JoinNormalizer(Metadata metadata, TypeAnalyzer typeAnalyzer, Session session, SymbolAllocator symbolAllocator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.session = requireNonNull(session, "session is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
    }

    public JoinNode tryNormalizeToOuterToInnerJoin(JoinNode node, Expression inheritedPredicate)
    {
        checkArgument(EnumSet.of(INNER, RIGHT, LEFT, FULL).contains(node.getType()), "Unsupported join type: %s", node.getType());

        if (node.getType() == JoinNode.Type.INNER) {
            return node;
        }

        if (node.getType() == JoinNode.Type.FULL) {
            boolean canConvertToLeftJoin = canConvertOuterToInner(node.getLeft().getOutputSymbols(), inheritedPredicate);
            boolean canConvertToRightJoin = canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate);
            if (!canConvertToLeftJoin && !canConvertToRightJoin) {
                return node;
            }
            if (canConvertToLeftJoin && canConvertToRightJoin) {
                return new JoinNode(node.getId(), INNER, node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputSymbols(), node.getFilter(), node.getLeftHashSymbol(), node.getRightHashSymbol(), node.getDistributionType(), node.isSpillable(), node.getDynamicFilters(), node.getReorderJoinStatsAndCost());
            }
            else {
                return new JoinNode(node.getId(), canConvertToLeftJoin ? LEFT : RIGHT,
                        node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputSymbols(), node.getFilter(), node.getLeftHashSymbol(), node.getRightHashSymbol(), node.getDistributionType(), node.isSpillable(), node.getDynamicFilters(), node.getReorderJoinStatsAndCost());
            }
        }

        if (node.getType() == JoinNode.Type.LEFT && !canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate) ||
                node.getType() == JoinNode.Type.RIGHT && !canConvertOuterToInner(node.getLeft().getOutputSymbols(), inheritedPredicate)) {
            return node;
        }
        return new JoinNode(node.getId(), JoinNode.Type.INNER, node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputSymbols(), node.getFilter(), node.getLeftHashSymbol(), node.getRightHashSymbol(), node.getDistributionType(), node.isSpillable(), node.getDynamicFilters(), node.getReorderJoinStatsAndCost());
    }

    public boolean canConvertOuterToInner(List<Symbol> innerSymbolsForOuterJoin, Expression inheritedPredicate)
    {
        Set<Symbol> innerSymbols = ImmutableSet.copyOf(innerSymbolsForOuterJoin);
        for (Expression conjunct : extractConjuncts(inheritedPredicate)) {
            if (isDeterministic(conjunct, metadata)) {
                // Ignore a conjunct for this test if we cannot deterministically get responses from it
                Object response = nullInputEvaluator(innerSymbols, conjunct);
                if (response == null || response instanceof NullLiteral || Boolean.FALSE.equals(response)) {
                    // If there is a single conjunct that returns FALSE or NULL given all NULL inputs for the inner side symbols of an outer join
                    // then this conjunct removes all effects of the outer join, and effectively turns this into an equivalent of an inner join.
                    // So, let's just rewrite this join as an INNER join
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Evaluates an expression's response to binding the specified input symbols to NULL
     */
    private Object nullInputEvaluator(final Collection<Symbol> nullSymbols, Expression expression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);
        return ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes)
                .optimize(symbol -> nullSymbols.contains(symbol) ? null : symbol.toSymbolReference());
    }
}
