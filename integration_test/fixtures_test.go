package integration_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/mitchellh/mapstructure"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// TestPayloads contains all the test payloads for different event types
// This might be refactored later if we generate payloads directly in tests.
type TestPayloads struct {
	// Chat related payloads
	ChatUpsert  []byte
	ChatUpdate  []byte
	ChatHistory []byte

	// Message related payloads
	MessageUpsert  []byte
	MessageUpdate  []byte
	MessageHistory []byte

	// Contact related payloads (add if needed)
	ContactUpsert  []byte
	ContactUpdate  []byte
	ContactHistory []byte

	// Agent related payloads (add if needed)
	AgentUpsert []byte
}

// generateTestPayloads uses model factories to create realistic test data
// and marshals it into the TestPayloads struct.
// NOTE: This is a placeholder structure. The actual implementation will
// depend on the available factories in internal/model/factories.go.
// We might create payloads directly within each test instead of using this.
func generateTestPayloads() (*TestPayloads, error) {
	payloads := &TestPayloads{}
	var err error

	// --- Generate Chat Payloads (Example) ---
	chat1 := model.Chat{ /* ... initialize fields if needed ... */ }
	payloads.ChatUpsert, err = json.Marshal(chat1)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal chat upsert: %w", err)
	}

	// Placeholder for Chat Update
	// ...

	// Placeholder for Chat History
	// ...

	// --- Generate Message Payloads (Example) ---
	msg1 := model.Message{ /* ... initialize fields if needed ... */ }
	payloads.MessageUpsert, err = json.Marshal(msg1)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message upsert: %w", err)
	}

	// --- Add generation for other payloads (MessageUpdate, MessageHistory, etc.) ---
	// --- based on available factories in internal/model/factories.go     ---

	fmt.Println("Generated test payloads using factories (example)")
	return payloads, nil
}

// --- Model Struct Generation ---

// generateModelStruct creates an instance of a model struct (e.g., model.Chat) using its factory.
// It allows overriding default fields by passing a struct of the same type with desired values set.
func generateModelStruct(modelName string, overrides ...interface{}) (interface{}, error) {
	var overrideStruct interface{}
	if len(overrides) > 0 {
		overrideStruct = overrides[0]
	}

	switch modelName {
	case "Agent":
		var agentOverride *model.Agent
		if ovr, ok := overrideStruct.(*model.Agent); ok {
			agentOverride = ovr
		}
		return model.NewAgent(agentOverride), nil
	case "Chat":
		var chatOverride *model.Chat
		if ovr, ok := overrideStruct.(*model.Chat); ok {
			chatOverride = ovr
		}
		return model.NewChat(chatOverride), nil
	case "Contact":
		var contactOverride *model.Contact
		if ovr, ok := overrideStruct.(*model.Contact); ok {
			contactOverride = ovr
		}
		return model.NewContact(contactOverride), nil
	case "Message":
		var messageOverride *model.Message
		if ovr, ok := overrideStruct.(*model.Message); ok {
			messageOverride = ovr
		}
		return model.NewMessage(messageOverride), nil
	case "OnboardingLog":
		var logOverride *model.OnboardingLog
		if ovr, ok := overrideStruct.(*model.OnboardingLog); ok {
			logOverride = ovr
		}
		return model.NewOnboardingLog(logOverride), nil
	// Add cases for other models as needed
	default:
		return nil, fmt.Errorf("unknown model name: %s", modelName)
	}
}

// CustomDecodeHook handles conversion from []byte (datatypes.JSON) to map[string]interface{} or *Struct types.
func unmarshalJSONDataHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		// Check if the source data is []byte
		if f.Kind() != reflect.Slice || f.Elem().Kind() != reflect.Uint8 {
			return data, nil
		}

		byteSlice, ok := data.([]byte)
		if !ok {
			// This path should ideally not be hit due to the kind checks above
			return data, nil
		}

		// If the byte slice is empty or represents JSON "null", handle appropriately.
		if len(byteSlice) == 0 || string(byteSlice) == "null" {
			if t.Kind() == reflect.Map {
				return reflect.MakeMap(t).Interface(), nil // Return an empty map
			} else if t.Kind() == reflect.Ptr {
				return reflect.Zero(t).Interface(), nil // Return nil for pointer types
			}
			// For other types, if it's an empty/null JSON, it might be an error or needs specific handling.
			// For now, return data, but this might need refinement based on field requirements.
			return data, nil
		}

		// Attempt to unmarshal for target types: map[string]interface{} or pointer to struct
		if t.Kind() == reflect.Map && t.Key().Kind() == reflect.String && t.Elem().Kind() == reflect.Interface {
			var newMap map[string]interface{}
			if err := json.Unmarshal(byteSlice, &newMap); err != nil {
				return nil, fmt.Errorf("unmarshalJSONDataHookFunc: failed to json.Unmarshal to map[string]interface{} from '%s': %w", string(byteSlice), err)
			}
			return newMap, nil
		} else if t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct {
			// Create a new instance of the target struct type (e.g., *model.KeyPayload)
			structInstance := reflect.New(t.Elem()).Interface()
			if err := json.Unmarshal(byteSlice, structInstance); err != nil {
				return nil, fmt.Errorf("unmarshalJSONDataHookFunc: failed to json.Unmarshal to struct %s from '%s': %w", t.Elem().Name(), string(byteSlice), err)
			}
			return structInstance, nil
		}

		// If no specific conversion matched, return data as is.
		return data, nil
	}
}

// --- Payload Struct Generation ---

// generatePayloadStruct creates an instance of a NATS payload struct (e.g., model.UpsertChatPayload).
// It typically generates the underlying model struct first and then maps relevant fields.
// Overrides can be provided as a struct of the target payload type.
func generatePayloadStruct(payloadName string, overrides ...interface{}) (interface{}, error) {
	var baseModel interface{}
	var err error
	var targetPayload interface{}

	// Determine the base model and target payload type
	switch payloadName {
	case "UpsertAgentPayload":
		baseModel, err = generateModelStruct("Agent")
		targetPayload = &model.UpsertAgentPayload{}
	case "UpsertChatPayload", "HistoryChatPayload": // History uses same structure per chat
		baseModel, err = generateModelStruct("Chat")
		targetPayload = &model.UpsertChatPayload{}
	case "UpdateChatPayload":
		baseModel, err = generateModelStruct("Chat") // Need base model for ID, CompanyID
		targetPayload = &model.UpdateChatPayload{}
	case "UpsertContactPayload", "HistoryContactPayload": // History uses same structure per contact
		baseModel, err = generateModelStruct("Contact")
		targetPayload = &model.UpsertContactPayload{}
	case "UpdateContactPayload":
		baseModel, err = generateModelStruct("Contact") // Need base model for ID, CompanyID
		targetPayload = &model.UpdateContactPayload{}
	case "UpsertMessagePayload", "HistoryMessagePayload": // History uses same structure per message
		baseModel, err = generateModelStruct("Message")
		targetPayload = &model.UpsertMessagePayload{}
	case "UpdateMessagePayload":
		baseModel, err = generateModelStruct("Message") // Need base model for ID, CompanyID
		targetPayload = &model.UpdateMessagePayload{}
	// Add cases for other payload structs
	default:
		return nil, fmt.Errorf("unknown payload name: %s", payloadName)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to generate base model for %s: %w", payloadName, err)
	}

	// Use mapstructure to map fields from the base model to the target payload struct.
	config := &mapstructure.DecoderConfig{
		Result:           targetPayload,
		WeaklyTypedInput: true,
		Squash:           true,   // Allows embedding
		TagName:          "json", // Use json tags for field names
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			unmarshalJSONDataHookFunc(),
			// Add other hooks if needed, e.g., string to time.Time, etc.
		),
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create mapstructure decoder: %w", err)
	}

	if err := decoder.Decode(baseModel); err != nil {
		// This warning indicates the hook might not have fully handled the conversion,
		// or there are other field mismatches mapstructure couldn't resolve.
		fmt.Printf("Warning: mapstructure.Decode failed for %s. Base: %T, Target: %T. Error: %v\n", payloadName, baseModel, targetPayload, err)

		// Limited Fallback manual mapping (should ideally be covered by a robust hook or correct model factories)
		switch b := baseModel.(type) {
		case *model.Chat:
			if pl, ok := targetPayload.(*model.UpsertChatPayload); ok {
				pl.ChatID = b.ChatID
				pl.CompanyID = b.CompanyID
				pl.Jid = b.Jid
				pl.PhoneNumber = b.PhoneNumber
				pl.PushName = b.PushName
				pl.IsGroup = b.IsGroup
				pl.ConversationTimestamp = b.ConversationTimestamp
				pl.UnreadCount = b.UnreadCount
				if b.LastMessageObj != nil && (pl.LastMessageObj == nil || len(pl.LastMessageObj) == 0) {
					_ = json.Unmarshal(b.LastMessageObj, &pl.LastMessageObj) // Best effort
				}
			}
		case *model.Message:
			if pl, ok := targetPayload.(*model.UpsertMessagePayload); ok {
				pl.MessageID = b.MessageID
				pl.ChatID = b.ChatID
				pl.CompanyID = b.CompanyID
				pl.Jid = b.Jid
				pl.Flow = b.Flow
				pl.Status = b.Status
				pl.MessageTimestamp = b.MessageTimestamp
				if b.MessageObj != nil && (pl.MessageObj == nil || len(pl.MessageObj) == 0) {
					_ = json.Unmarshal(b.MessageObj, &pl.MessageObj) // Best effort
				}
				if b.Key != nil && pl.Key == nil {
					var tempKey model.KeyPayload
					if json.Unmarshal(b.Key, &tempKey) == nil {
						pl.Key = &tempKey
					}
				}
			}
		}
	}

	// Handle specific payload structures like History*
	switch pl := targetPayload.(type) {
	case *model.UpsertChatPayload:
		if payloadName == "HistoryChatPayload" {
			// History payloads expect a list
			historyPayload := &model.HistoryChatPayload{
				Chats: []model.UpsertChatPayload{*pl},
			}
			targetPayload = historyPayload // Replace target with the history structure
		}
	case *model.UpsertContactPayload:
		if payloadName == "HistoryContactPayload" {
			historyPayload := &model.HistoryContactPayload{
				Contacts: []model.UpsertContactPayload{*pl},
			}
			targetPayload = historyPayload
		}
	case *model.UpsertMessagePayload:
		if payloadName == "HistoryMessagePayload" {
			historyPayload := &model.HistoryMessagePayload{
				Messages: []model.UpsertMessagePayload{*pl},
			}
			targetPayload = historyPayload
		}
	}

	// Apply overrides if provided
	if len(overrides) > 0 && overrides[0] != nil {
		overrideValue := reflect.ValueOf(overrides[0])
		targetValue := reflect.ValueOf(targetPayload)

		// Ensure target is a pointer and override is the same type or a map
		if targetValue.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("targetPayload must be a pointer, got %T", targetPayload)
		}

		if overrideValue.Type() == targetValue.Type() {
			// If override is the same type, merge non-zero fields
			targetElem := targetValue.Elem()
			overrideElem := overrideValue.Elem()
			for i := 0; i < targetElem.NumField(); i++ {
				overrideField := overrideElem.Field(i)
				if !overrideField.IsZero() { // Only copy non-zero fields from override
					targetElem.Field(i).Set(overrideField)
				}
			}
		} else if overrideValue.Kind() == reflect.Map {
			// If override is a map, use mapstructure to decode it onto the target
			// Re-create decoder config with the hook for this specific map override decoding
			mapDecoderConfig := &mapstructure.DecoderConfig{
				Result:           targetPayload, // Decode directly into the target struct
				TagName:          "json",
				WeaklyTypedInput: true,
				DecodeHook: mapstructure.ComposeDecodeHookFunc(
					unmarshalJSONDataHookFunc(),
				),
			}
			mapDecoder, mapErr := mapstructure.NewDecoder(mapDecoderConfig)
			if mapErr != nil {
				return nil, fmt.Errorf("failed to create mapstructure decoder for override map: %w", mapErr)
			}
			if mapDecodeErr := mapDecoder.Decode(overrides[0]); mapDecodeErr != nil {
				return nil, fmt.Errorf("failed to decode override map onto payload struct %s: %w", payloadName, mapDecodeErr)
			}
		} else {
			return nil, fmt.Errorf("unsupported override type %T for payload %s", overrides[0], payloadName)
		}
	}

	return targetPayload, nil
}

// --- NATS Payload Generation (Bytes) ---

// subjectToPayloadMap maps base NATS subjects to their corresponding payload struct names.
var subjectToPayloadMap = map[string]string{
	"v1.connection.update": "UpsertAgentPayload",
	"v1.chats.upsert":      "UpsertChatPayload",
	"v1.chats.update":      "UpdateChatPayload",
	"v1.history.chats":     "HistoryChatPayload",
	"v1.contacts.upsert":   "UpsertContactPayload",
	"v1.contacts.update":   "UpdateContactPayload",
	"v1.history.contacts":  "HistoryContactPayload",
	"v1.messages.upsert":   "UpsertMessagePayload",
	"v1.messages.update":   "UpdateMessagePayload",
	"v1.history.messages":  "HistoryMessagePayload",
	// Add other mappings
}

// generateNatsPayload creates a JSON byte slice payload for a given NATS subject.
// It generates the corresponding payload struct, applies overrides, marshals to JSON,
// and validates against the schema.
func generateNatsPayload(subject string, overrides ...interface{}) ([]byte, error) {
	// 1. Determine Base Subject and Payload Type
	parts := strings.Split(subject, ".")
	baseSubject := subject
	companyIDFromSubject := "" // Extracted tenant/company ID from the subject suffix
	if len(parts) > 1 {
		lastPart := parts[len(parts)-1]
		// A more robust check might be needed depending on actual tenant ID formats
		if strings.Contains(lastPart, "_tenant_") || strings.Contains(lastPart, "_company_") || strings.Contains(lastPart, DefaultCompanyID) {
			baseSubject = strings.Join(parts[:len(parts)-1], ".")
			companyIDFromSubject = lastPart
		}
	}

	payloadName, ok := subjectToPayloadMap[baseSubject]
	if !ok {
		// Check if the subject itself is a direct key (e.g., "v1.connection.update")
		payloadName, ok = subjectToPayloadMap[subject]
		if !ok {
			return nil, fmt.Errorf("no payload mapping found for base subject: %s (derived from %s)", baseSubject, subject)
		}
		baseSubject = subject // Use the full subject if it was a direct key
	}

	// 2. Generate Payload Struct with Overrides
	// Pass the extracted companyID to ensure it's set if needed, overrides can still change it.
	initialOverrides := map[string]interface{}{}
	if companyIDFromSubject != "" {
		initialOverrides["CompanyID"] = companyIDFromSubject
	}

	// Merge initial overrides with provided overrides
	// Provided overrides take precedence
	var finalOverrideArgument interface{}  // Argument to pass to generatePayloadStruct
	mergedMapOverrides := initialOverrides // Start with base overrides (CompanyID)

	if len(overrides) > 0 && overrides[0] != nil {
		if overrideMap, ok := overrides[0].(map[string]interface{}); ok {
			// If it's a map, merge it with initialOverrides
			for k, v := range overrideMap {
				mergedMapOverrides[k] = v
			}
			finalOverrideArgument = mergedMapOverrides // Pass the merged map
		} else {
			// If it's not a map, assume it's the target struct type or similar.
			// Pass it directly. generatePayloadStruct handles struct merging.
			// We also need to apply the initialOverrides (CompanyID) in generatePayloadStruct if this path is taken.
			// For simplicity now, we prioritize the direct struct override.
			// A more complex merge could be done here if needed.
			finalOverrideArgument = overrides[0]
			// Note: If overrides[0] is a struct, CompanyID from subject might be ignored if set in the struct.
		}
	} else {
		// No overrides provided, just use initialOverrides (CompanyID)
		finalOverrideArgument = mergedMapOverrides
	}

	// TODO: Add specific tests that pass edge case overrides here:
	// e.g., generateNatsPayload(subject, map[string]interface{}{"push_name": nil, "unread_count": 0})
	// e.g., generateNatsPayload(subject, map[string]interface{}{"message_obj": map[string]interface{}{}})

	payloadStruct, err := generatePayloadStruct(payloadName, finalOverrideArgument) // Pass the determined override argument
	if err != nil {
		return nil, fmt.Errorf("failed to generate payload struct for %s: %w", payloadName, err)
	}

	// Redundant CompanyID check (can likely be removed as generatePayloadStruct handles it)
	/*
		if companyIDFromSubject != "" {
			payloadValue := reflect.ValueOf(payloadStruct)
			if payloadValue.Kind() == reflect.Ptr {
				payloadElem := payloadValue.Elem()
				if payloadElem.Kind() == reflect.Struct {
					companyField := payloadElem.FieldByName("CompanyID")
					if companyField.IsValid() && companyField.Kind() == reflect.String {
						if companyField.String() == "" {
							companyField.SetString(companyIDFromSubject)
						}
					}
				}
			}
		}
	*/

	// 3. Marshal to JSON
	payloadBytes, err := json.Marshal(payloadStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload struct %s to JSON: %w", payloadName, err)
	}

	// 4. Validate against JSON Schema
	// Re-enabled schema validation
	// fmt.Printf("WARN: Skipping schema validation for generated payload (Subject: %s, Base: %s)\n", subject, baseSubject)
	schemaName, ok := SubjectToSchemaMap[baseSubject]
	if !ok {
		return nil, fmt.Errorf("no schema mapping found for base subject: %s (derived from %s) during validation lookup", baseSubject, subject)
	}

	if err := validatePayload(payloadBytes, schemaName); err != nil {
		// Include payload in error for easier debugging
		payloadStr := string(payloadBytes)
		if len(payloadStr) > 500 {
			payloadStr = payloadStr[:500] + "... (truncated)"
		}
		return payloadBytes, fmt.Errorf("payload validation failed for subject %s (schema %s): %w\nPayload: %s", subject, schemaName, err, payloadStr)
	}

	return payloadBytes, nil
}
