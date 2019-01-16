package launcher_test

import (
	"bytes"
	"fmt"
	"io"
	nethttp "net/http"
	"testing"
	"time"

	phttp "github.com/influxdata/influxdb/http"
)

func TestPipeline_Write_Query_FieldKey(t *testing.T) {

	be := RunLauncherOrFail(t, ctx)
	be.SetupOrFail(t)
	defer be.ShutdownOrFail(t, ctx)

	resp, err := nethttp.DefaultClient.Do(
		be.MustNewHTTPRequest(
			"POST",
			fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", be.Org.ID, be.Bucket.ID),
			`cpu,region=west,server=a v0=1.2
cpu,region=west,server=b v0=33.2
cpu,region=east,server=b,area=z v1=100.0
disk,regions=north,server=b v1=101.2
mem,server=b value=45.2`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()
	if resp.StatusCode != 204 {
		t.Fatal("failed call to write points")
	}

	rawQ := fmt.Sprintf(`from(bucket:"%s")
	|> filter(fn: (r) => r._m ==  "cpu" and (r._f == "v1" or r._f == "v0"))
	|> range(start:-1m)
	`, be.Bucket.Name)

	results := be.MustExecuteQuery(be.Org.ID, rawQ, be.Auth)
	defer results.Done()
	results.First(t).HasTablesWithCols([]int{4, 4, 5})
}

// This test initialises a default launcher writes some data,
// and checks that the queried results contain the expected number of tables
// and expected number of columns.
func TestPipeline_WriteV2_Query(t *testing.T) {
	t.Parallel()

	be := RunLauncherOrFail(t, ctx)
	be.SetupOrFail(t)
	defer be.ShutdownOrFail(t, ctx)

	// The default gateway instance inserts some values directly such that ID lookups seem to break,
	// so go the roundabout way to insert things correctly.
	req := be.MustNewHTTPRequest(
		"POST",
		fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", be.Org.ID, be.Bucket.ID),
		fmt.Sprintf("ctr n=1i %d", time.Now().UnixNano()),
	)
	phttp.SetToken(be.Auth.Token, req)

	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()

	if resp.StatusCode != nethttp.StatusNoContent {
		buf := new(bytes.Buffer)
		if _, err := io.Copy(buf, resp.Body); err != nil {
			t.Fatalf("Could not read body: %s", err)
		}
		t.Fatalf("exp status %d; got %d, body: %s", nethttp.StatusNoContent, resp.StatusCode, buf.String())
	}

	res := be.MustExecuteQuery(
		be.Org.ID,
		fmt.Sprintf(`from(bucket:"%s") |> range(start:-5m)`, be.Bucket.Name),
		be.Auth)
	defer res.Done()
	res.HasTableCount(t, 1)
}
