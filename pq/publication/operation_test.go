package publication

import "testing"

func TestOperationsValidateRejectsInvalidContracts(t *testing.T) {
	tests := []Operations{
		nil,
		{Operation("UPSERT")},
		{OperationInsert, OperationInsert},
	}
	for _, operations := range tests {
		if err := operations.Validate(); err == nil {
			t.Fatalf("Operations(%v).Validate() succeeded", operations)
		}
	}
	if err := (Operations{OperationInsert, OperationUpdate, OperationDelete, OperationTruncate}).Validate(); err != nil {
		t.Fatal(err)
	}
}
