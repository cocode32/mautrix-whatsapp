// Custom handlers for mautrix-whatsapp bridge
// This file contains all CocoCode customizations to make merging with upstream easier.
// All handlers in this file send events ONLY to Matrix (local side) and never back to WhatsApp.
//
// Features:
// - Message revoke notices (instead of deleting messages)
// - Message edit notices (instead of replacing messages)
// - Delivery status reactions (📭 sent, 📩 delivered, 👀 read)
// - Profile picture update notices (with image preview)

package connector

import (
	"bytes"
	"context"
	"fmt"
	"image"
	_ "image/jpeg" // Register JPEG decoder
	_ "image/png"  // Register PNG decoder
	"io"
	"net/http"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// sendMatrixMessage is a helper that sends a message to a Matrix room using BotIntent.
// This ensures all custom messages are local-only and never go back to WhatsApp.
func (wa *WhatsAppClient) sendMatrixMessage(ctx context.Context, roomID id.RoomID, content *event.MessageEventContent) error {
	_, err := wa.Main.Bridge.Matrix.BotIntent().SendMessage(ctx, roomID, event.EventMessage, &event.Content{
		Parsed: content,
	}, nil)
	return err
}

// sendMatrixReaction is a helper that sends a reaction to a Matrix message using BotIntent.
func (wa *WhatsAppClient) sendMatrixReaction(ctx context.Context, roomID id.RoomID, targetEventID id.EventID, emoji string) error {
	_, err := wa.Main.Bridge.Matrix.BotIntent().SendMessage(ctx, roomID, event.EventReaction, &event.Content{
		Parsed: &event.ReactionEventContent{
			RelatesTo: event.RelatesTo{
				Type:    event.RelAnnotation,
				EventID: targetEventID,
				Key:     emoji,
			},
		},
	}, nil)
	return err
}

// handleMatrixMessageRevoke sends a notice to Matrix when a message is revoked.
// This preserves the original message and adds a reply indicating it was revoked.
// Uses direct Matrix API - this is a LOCAL event only, never goes to WhatsApp.
func (wa *WhatsAppClient) handleMatrixMessageRevoke(ctx context.Context, evt *events.Message) bool {
	protocolMsg := evt.Message.GetProtocolMessage()
	if protocolMsg == nil || protocolMsg.GetKey() == nil {
		return true
	}

	revokedKey := protocolMsg.GetKey()
	revokedMsgID := revokedKey.GetID()

	log := wa.UserLogin.Log.With().
		Str("action", "coco_revoke_notice").
		Str("revoked_message_id", revokedMsgID).
		Stringer("chat", evt.Info.Chat).
		Logger()

	// Determine sender JID for message lookup
	senderJID := types.JID{
		User:   revokedKey.GetRemoteJID(),
		Server: types.DefaultUserServer,
	}
	if revokedKey.GetFromMe() {
		senderJID = evt.Info.Sender
	}

	// Determine who revoked the message (for display name)
	var revokerName string
	if evt.Info.IsFromMe {
		revokerName = "You"
	} else {
		revokerName = evt.Info.PushName
		if revokerName == "" {
			revokerName = evt.Info.Sender.User
		}
	}

	log.Info().Str("revoker", revokerName).Msg("Message was revoked - sending local Matrix notice")

	// Get the portal
	portalKey := wa.makeWAPortalKey(evt.Info.Chat)
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil {
		log.Err(err).Msg("Failed to get portal for revoke notice")
		return true
	}
	if portal == nil {
		log.Debug().Msg("Portal doesn't exist, skipping revoke notice")
		return true
	}

	// Find the original message in the database to reply to it
	targetMsgID := waid.MakeMessageID(evt.Info.Chat, senderJID, revokedMsgID)
	message, err := wa.Main.Bridge.DB.Message.GetFirstPartByID(ctx, portalKey.Receiver, targetMsgID)
	if err != nil {
		log.Err(err).Msg("Failed to get original message from database")
	}

	// Build the notice content
	noticeText := fmt.Sprintf("⚠️ %s revoked a message", revokerName)
	content := &event.MessageEventContent{
		MsgType: event.MsgNotice,
		Body:    noticeText,
	}

	// If we found the original message, make this a reply to it
	if message != nil {
		content.RelatesTo = &event.RelatesTo{
			InReplyTo: &event.InReplyTo{
				EventID: message.MXID,
			},
		}
	}

	if err := wa.sendMatrixMessage(ctx, portal.MXID, content); err != nil {
		log.Err(err).Msg("Failed to send revoke notice to Matrix")
	} else {
		log.Debug().Msg("Sent revoke notice to Matrix")
	}

	return true
}

// handleMatrixMessageEdit sends a notice to Matrix when a message is edited.
// This preserves the original message and shows what it was edited to as a reply.
// Uses direct Matrix API - this is a LOCAL event only, never goes to WhatsApp.
func (wa *WhatsAppClient) handleMatrixMessageEdit(ctx context.Context, evt *events.Message) bool {
	protocolMsg := evt.Message.GetProtocolMessage()
	if protocolMsg == nil || protocolMsg.GetKey() == nil {
		return true
	}

	editedKey := protocolMsg.GetKey()
	editedMsgID := editedKey.GetID()
	editedMessage := protocolMsg.GetEditedMessage()

	log := wa.UserLogin.Log.With().
		Str("action", "coco_edit_notice").
		Str("edited_message_id", editedMsgID).
		Stringer("chat", evt.Info.Chat).
		Logger()

	// Determine sender JID for message lookup
	senderJID := types.JID{
		User:   editedKey.GetRemoteJID(),
		Server: types.DefaultUserServer,
	}
	if editedKey.GetFromMe() {
		senderJID = evt.Info.Sender
	}

	// Determine who edited the message (for display name)
	var editorName string
	if evt.Info.IsFromMe {
		editorName = "You"
	} else {
		editorName = evt.Info.PushName
		if editorName == "" {
			editorName = evt.Info.Sender.User
		}
	}

	// Extract the new message content
	var newContent string
	if editedMessage != nil {
		if editedMessage.GetConversation() != "" {
			newContent = editedMessage.GetConversation()
		} else if editedMessage.GetExtendedTextMessage() != nil {
			newContent = editedMessage.GetExtendedTextMessage().GetText()
		} else if editedMessage.GetImageMessage() != nil {
			newContent = "[Image] " + editedMessage.GetImageMessage().GetCaption()
		} else if editedMessage.GetVideoMessage() != nil {
			newContent = "[Video] " + editedMessage.GetVideoMessage().GetCaption()
		} else if editedMessage.GetDocumentMessage() != nil {
			newContent = "[Document] " + editedMessage.GetDocumentMessage().GetCaption()
		} else {
			newContent = "[Edited media message]"
		}
	}

	if newContent == "" {
		newContent = "[Empty or unsupported edit]"
	}

	log.Info().
		Str("editor", editorName).
		Str("new_content_preview", cocoTruncateString(newContent, 50)).
		Msg("Message was edited - sending local Matrix notice")

	// Get the portal
	portalKey := wa.makeWAPortalKey(evt.Info.Chat)
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil {
		log.Err(err).Msg("Failed to get portal for edit notice")
		return true
	}
	if portal == nil {
		log.Debug().Msg("Portal doesn't exist, skipping edit notice")
		return true
	}

	// Find the original message in the database to reply to it
	targetMsgID := waid.MakeMessageID(evt.Info.Chat, senderJID, editedMsgID)
	message, err := wa.Main.Bridge.DB.Message.GetFirstPartByID(ctx, portalKey.Receiver, targetMsgID)
	if err != nil {
		log.Err(err).Msg("Failed to get original message from database")
	}

	// Build the notice content
	noticeText := fmt.Sprintf("✏️ %s edited this message:\n\n%s", editorName, newContent)
	content := &event.MessageEventContent{
		MsgType: event.MsgNotice,
		Body:    noticeText,
	}

	// If we found the original message, make this a reply to it
	if message != nil {
		content.RelatesTo = &event.RelatesTo{
			InReplyTo: &event.InReplyTo{
				EventID: message.MXID,
			},
		}
	}

	if err := wa.sendMatrixMessage(ctx, portal.MXID, content); err != nil {
		log.Err(err).Msg("Failed to send edit notice to Matrix")
	} else {
		log.Debug().Msg("Sent edit notice to Matrix")
	}

	return true
}

// sendMatrixDeliveryReaction sends a Matrix reaction to indicate message delivery/read status.
// Emoji mapping: 📭 = sent, 📩 = delivered, 👀 = read
// Uses direct Matrix API - this is a LOCAL event only, never goes to WhatsApp.
func (wa *WhatsAppClient) sendMatrixDeliveryReaction(ctx context.Context, evt *events.Receipt) {
	log := wa.UserLogin.Log.With().
		Str("action", "coco_delivery_reaction").
		Stringer("chat", evt.Chat).
		Str("receipt_type", string(evt.Type)).
		Logger()

	// Determine which emoji to use based on receipt type
	var emoji string
	switch evt.Type {
	case types.ReceiptTypeSender:
		emoji = "📭" // Sent - mailbox with flag down
	case types.ReceiptTypeDelivered:
		emoji = "📩" // Delivered - envelope with arrow
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf:
		emoji = "👀" // Read - eyes
	default:
		return
	}

	// Get the portal
	portalKey := wa.makeWAPortalKey(evt.Chat)
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil {
		log.Err(err).Msg("Failed to get portal for delivery reaction")
		return
	}
	if portal == nil {
		log.Debug().Msg("Portal doesn't exist, skipping delivery reaction")
		return
	}

	messageSender := wa.JID

	for _, msgID := range evt.MessageIDs {
		targetMsgID := waid.MakeMessageID(evt.Chat, messageSender, msgID)

		message, err := wa.Main.Bridge.DB.Message.GetFirstPartByID(ctx, portalKey.Receiver, targetMsgID)
		if err != nil {
			log.Err(err).Str("message_id", msgID).Msg("Failed to get message from database")
			continue
		}
		if message == nil {
			log.Debug().Str("message_id", msgID).Msg("Message not found in database")
			continue
		}

		if err := wa.sendMatrixReaction(ctx, portal.MXID, message.MXID, emoji); err != nil {
			log.Err(err).
				Str("message_id", msgID).
				Str("emoji", emoji).
				Msg("Failed to send delivery status reaction")
		} else {
			log.Debug().
				Str("message_id", msgID).
				Str("emoji", emoji).
				Str("matrix_event_id", string(message.MXID)).
				Msg("Sent delivery status reaction to Matrix")
		}
	}
}

// sendMatrixPictureUpdateNotice sends a profile picture update notice to Matrix.
// If picture was updated, includes the image with proper dimensions for inline display.
// If removed, sends text notice.
// Uses direct Matrix API - this is a LOCAL event only, never goes to WhatsApp.
func (wa *WhatsAppClient) sendMatrixPictureUpdateNotice(ctx context.Context, evt *events.Picture) {
	log := wa.UserLogin.Log.With().
		Str("action", "coco_picture_notice").
		Stringer("jid", evt.JID).
		Str("picture_id", evt.PictureID).
		Bool("removed", evt.Remove).
		Logger()

	// Normalize JID to phone number for portal lookup
	portalJID := evt.JID
	if evt.JID.Server == types.HiddenUserServer {
		pn, err := wa.GetStore().LIDs.GetPNForLID(ctx, evt.JID)
		if err != nil {
			log.Err(err).Msg("Failed to get phone number for LID in picture notice")
		} else if !pn.IsEmpty() {
			portalJID = pn
			log.Debug().
				Stringer("original_jid", evt.JID).
				Stringer("normalized_jid", portalJID).
				Msg("Normalized LID to phone number for portal lookup")
		}
	}

	// Get the portal
	portalKey := wa.makeWAPortalKey(portalJID)
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil {
		log.Err(err).Msg("Failed to get portal for picture notice")
		return
	}
	if portal == nil {
		log.Debug().Msg("Portal doesn't exist, skipping picture notice")
		return
	}

	// Get the contact name
	userInfo, _ := wa.getUserInfo(ctx, evt.JID, false)
	contactName := *userInfo.Name

	if evt.Remove {
		// Picture was removed - send a text notice
		noticeText := fmt.Sprintf("📷 %s removed their profile picture", contactName)
		content := &event.MessageEventContent{
			MsgType: event.MsgNotice,
			Body:    noticeText,
		}

		if err := wa.sendMatrixMessage(ctx, portal.MXID, content); err != nil {
			log.Err(err).Msg("Failed to send picture removal notice to Matrix")
		} else {
			log.Info().Msg("Sent profile picture removal notice")
		}
		return
	}

	// Picture was updated - fetch and send the full picture
	pictureInfo, err := wa.Client.GetProfilePictureInfo(ctx, evt.JID, &whatsmeow.GetProfilePictureParams{
		Preview: false, // Get full resolution
	})
	if err != nil {
		log.Err(err).Msg("Failed to get profile picture info")
		return
	}
	if pictureInfo == nil {
		log.Warn().Msg("Profile picture info is nil")
		return
	}

	// Download the picture
	pictureBytes, err := cocoDownloadProfilePicture(ctx, pictureInfo.URL)
	if err != nil {
		log.Err(err).Msg("Failed to download profile picture")
		return
	}

	// a file name is necessary to display in the matrix client correctly
	fileName := fmt.Sprintf("new_profile_picture.jpg")
	// Get image dimensions for proper Matrix display
	width, height := cocoGetImageDimensions(pictureBytes)
	// this is a message that is used as the caption for the picture
	noticeText := fmt.Sprintf("📷 %s updated their profile picture", contactName)

	// Upload to Matrix
	contentUri, _, err := wa.Main.Bridge.Matrix.BotIntent().UploadMedia(ctx, "", pictureBytes, "profile_picture.jpg", "image/jpeg")
	if err != nil {
		log.Err(err).Msg("Failed to upload profile picture to Matrix")
		return
	}

	// send image to chat
	content := &event.MessageEventContent{
		MsgType:  event.MsgImage,
		Body:     noticeText,
		URL:      contentUri,
		FileName: fileName,
		Info: &event.FileInfo{
			MimeType: "image/jpeg",
			Size:     len(pictureBytes),
			Width:    width,
			Height:   height,
		},
	}

	if err := wa.sendMatrixMessage(ctx, portal.MXID, content); err != nil {
		log.Err(err).Msg("Failed to send picture update notice to Matrix")
	} else {
		log.Info().
			Int("width", width).
			Int("height", height).
			Msg("Sent profile picture update with image")
	}
}

// cocoTruncateString truncates a string to maxLen characters and adds "..." if truncated
func cocoTruncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// cocoDownloadProfilePicture downloads a profile picture from a URL
func cocoDownloadProfilePicture(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

// cocoGetImageDimensions decodes image bytes to get width and height.
// Returns (0, 0) if dimensions cannot be determined (image will still display, just without size hints).
func cocoGetImageDimensions(data []byte) (width, height int) {
	config, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return 0, 0
	}
	return config.Width, config.Height
}

// ShouldInterceptRevoke returns true if this is a revoke message that should be handled
// by custom logic instead of the default behavior
func (wa *WhatsAppClient) ShouldInterceptRevoke(parsedMessageType string) bool {
	return parsedMessageType == "revoke"
}

// ShouldInterceptEdit returns true if this is an edit message that should be handled
// by custom logic instead of the default behavior
func (wa *WhatsAppClient) ShouldInterceptEdit(parsedMessageType string) bool {
	return parsedMessageType == "edit"
}
