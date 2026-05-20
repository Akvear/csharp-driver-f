using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    /// <summary>
    /// Tests async (IAsyncEnumerable) and sync (IEnumerable) iteration
    /// over result sets that span multiple pages.
    /// </summary>
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class MultiPageAsyncIterationTests : SharedClusterTest
    {
        private const int RowCount = 1000;
        // Page size is small enough to make it likely that the fast path of the `BridgedRowSet.NextRow()`,
        // `row_set_try_next_row_sync()`, will sometimes return `SyncNextRowResult.NeedAsync`,
        // so that the fallback async path (`BridgedRowSet.NextRowAsync()`) is exercised too.
        private const int PageSize = 7;
        private string _tableName;

        public MultiPageAsyncIterationTests()
            : base(1, createSession: true) { }

        [OneTimeSetUp]
        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();

            _tableName = "tbl" + Guid.NewGuid().ToString("N").ToLower();
            Session.Execute(
                $"CREATE TABLE {KeyspaceName}.{_tableName} (" +
                "id int PRIMARY KEY, " +
                "value text)");

            // Insert rows. Use simple statements in a loop — batches
            // are not yet fully supported in the Rust bridge.
            for (int i = 0; i < RowCount; i++)
            {
                Session.Execute(
                    $"INSERT INTO {KeyspaceName}.{_tableName} " +
                    $"(id, value) VALUES ({i}, 'v{i}')");
            }
        }

        [Test]
        public async Task AsyncIteration_ReturnsAllRows()
        {
            var statement = new SimpleStatement(
                $"SELECT * FROM {KeyspaceName}.{_tableName}");
            statement.SetPageSize(PageSize);

            var rowSet = await Session.ExecuteAsync(statement);

            var ids = new HashSet<int>();
            await foreach (var row in rowSet)
            {
                ids.Add(row.GetValue<int>("id"));
            }

            Assert.AreEqual(RowCount, ids.Count,
                "Async iteration should return all rows across pages");
        }

        [Test]
        public void SyncIteration_ReturnsAllRows()
        {
            var statement = new SimpleStatement(
                $"SELECT * FROM {KeyspaceName}.{_tableName}");
            statement.SetPageSize(PageSize);

            var rowSet = Session.Execute(statement);

            var ids = new HashSet<int>();
            foreach (var row in rowSet)
            {
                ids.Add(row.GetValue<int>("id"));
            }

            Assert.AreEqual(RowCount, ids.Count,
                "Sync iteration should return all rows across pages");
        }
    }
}
