package helper

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func GetOrganizationInstance(organizationID string, organizationInstanceID int) ([]byte, error) {
	// Buat HTTP client
	client := &http.Client{}

	// Buat URL dengan organization instance ID
	url := fmt.Sprintf("https://dev.api.moslem101.com/identity/v1/organization-instance/%d", organizationInstanceID)

	// Buat request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	// Set headers
	req.Header.Set("accept", "*/*")
	req.Header.Set("x-organization-id", organizationID)
	req.Header.Set("Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJodHRwczovL2Rldi5hcGkubW9zbGVtMTAxLmNvbSIsImlzcyI6IjEwMV9JZGVudGl0eV9Jc3N1ZXIiLCJpZCI6ImNkNTBjMjZhLTA4ODEtNDdkYS1hMjNmLTFiM2IyZGM3OGRiYyIsInJvbGUiOiJzdXBlcl9hZG1pbiIsInVzZXJuYW1lIjoiYXBlc2pzIiwiZW1haWwiOiJhc2VwamFlbnVkaW5zdXRhcmppQGdtYWlsLmNvbSIsImlhdCI6MTczNTUyMTY0NCwiZXhwIjoxNzM3MjU3MzMzfQ.9e6NDOFkMfbPTeLma9dXEI3cvoJiNZWHXXq5LkRzBDo")

	// Kirim request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// Baca response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	// Cek status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error response status: %d, body: %s", resp.StatusCode, string(body))
	}

	return body, nil
}
