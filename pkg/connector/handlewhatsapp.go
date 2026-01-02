// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2024 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/ptr"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

const (
	WANotLoggedIn      status.BridgeStateErrorCode = "wa-not-logged-in"
	WALoggedOut        status.BridgeStateErrorCode = "wa-logged-out"
	WAMainDeviceGone   status.BridgeStateErrorCode = "wa-main-device-gone"
	WAUnknownLogout    status.BridgeStateErrorCode = "wa-unknown-logout"
	WANotConnected     status.BridgeStateErrorCode = "wa-not-connected"
	WAConnecting       status.BridgeStateErrorCode = "wa-connecting"
	WAKeepaliveTimeout status.BridgeStateErrorCode = "wa-keepalive-timeout"
	WAPhoneOffline     status.BridgeStateErrorCode = "wa-phone-offline"
	WAConnectionFailed status.BridgeStateErrorCode = "wa-connection-failed"
	WADisconnected     status.BridgeStateErrorCode = "wa-transient-disconnect"
	WAStreamReplaced   status.BridgeStateErrorCode = "wa-stream-replaced"
	WAStreamError      status.BridgeStateErrorCode = "wa-stream-error"
	WAClientOutdated   status.BridgeStateErrorCode = "wa-client-outdated"
	WATemporaryBan     status.BridgeStateErrorCode = "wa-temporary-ban"
)

func init() {
	status.BridgeStateHumanErrors.Update(status.BridgeStateErrorMap{
		WALoggedOut:        "You were logged out from another device. Relogin to continue using the bridge.",
		WANotLoggedIn:      "You're not logged into WhatsApp. Relogin to continue using the bridge.",
		WAMainDeviceGone:   "Your phone was logged out from WhatsApp. Relogin to continue using the bridge.",
		WAUnknownLogout:    "You were logged out for an unknown reason. Relogin to continue using the bridge.",
		WANotConnected:     "You're not connected to WhatsApp",
		WAConnecting:       "Reconnecting to WhatsApp...",
		WAKeepaliveTimeout: "The WhatsApp web servers are not responding. The bridge will try to reconnect.",
		WAPhoneOffline:     "Your phone hasn't been seen in over 12 days. The bridge is currently connected, but will get disconnected if you don't open the app soon.",
		WAConnectionFailed: "Connecting to the WhatsApp web servers failed.",
		WADisconnected:     "Disconnected from WhatsApp. Trying to reconnect.",
		WAClientOutdated:   "Connect failure: 405 client outdated. Bridge must be updated.",
		WAStreamReplaced:   "Stream replaced: the bridge was started in another location.",
	})
}

func (wa *WhatsAppClient) handleWAEvent(rawEvt any) (success bool) {
	log := wa.UserLogin.Log
	ctx := log.WithContext(wa.Main.Bridge.BackgroundCtx)

	success = true
	switch evt := rawEvt.(type) {
	case *events.Message:
		success = wa.handleWAMessage(ctx, evt)
	case *events.Receipt:
		success = wa.handleWAReceipt(ctx, evt)
	case *events.ChatPresence:
		wa.handleWAChatPresence(ctx, evt)
	case *events.UndecryptableMessage:
		success = wa.handleWAUndecryptableMessage(ctx, evt)

	case *events.CallOffer:
		success = wa.handleWACallStart(ctx, evt.GroupJID, evt.CallCreator, evt.CallCreatorAlt, evt.CallID, "", evt.Timestamp)
	case *events.CallOfferNotice:
		success = wa.handleWACallStart(ctx, evt.GroupJID, evt.CallCreator, evt.CallCreatorAlt, evt.CallID, evt.Type, evt.Timestamp)
	case *events.CallTerminate, *events.CallRelayLatency, *events.CallAccept, *events.UnknownCallEvent:
		// ignore
	case *events.IdentityChange:
		wa.handleWAIdentityChange(ctx, evt)
	case *events.MarkChatAsRead:
		success = wa.handleWAMarkChatAsRead(ctx, evt)
	case *events.DeleteForMe:
		success = wa.handleWADeleteForMe(evt)
	case *events.DeleteChat:
		success = wa.handleWADeleteChat(evt)
	case *events.Mute:
		success = wa.handleWAMute(evt)
	case *events.Archive:
		success = wa.handleWAArchive(evt)
	case *events.Pin:
		success = wa.handleWAPin(evt)

	case *events.HistorySync:
		if wa.Main.Bridge.Config.Backfill.Enabled {
			wa.historySyncs <- evt.Data
		}
	case *events.MediaRetry:
		wa.phoneSeen(evt.Timestamp)
		success = wa.UserLogin.QueueRemoteEvent(&WAMediaRetry{MediaRetry: evt, wa: wa}).Success

	case *events.GroupInfo:
		success = wa.handleWAGroupInfoChange(ctx, evt)
	case *events.JoinedGroup:
		success = wa.handleWAJoinedGroup(ctx, evt)
	case *events.NewsletterJoin:
		success = wa.handleWANewsletterJoin(ctx, evt)
	case *events.NewsletterLeave:
		success = wa.handleWANewsletterLeave(evt)
	case *events.Picture:
		success = wa.handleWAPictureUpdate(ctx, evt)

	case *events.AppStateSyncComplete:
		if len(wa.GetStore().PushName) > 0 && evt.Name == appstate.WAPatchCriticalBlock {
			err := wa.updatePresence(ctx, types.PresenceUnavailable)
			if err != nil {
				log.Warn().Err(err).Msg("Failed to send presence after app state sync")
			}
			go wa.syncRemoteProfile(log.WithContext(context.Background()), nil)
		} else if evt.Name == appstate.WAPatchCriticalUnblockLow {
			go wa.resyncContacts(false, true)
		}
	case *events.AppState:
		// Intentionally ignored
	case *events.PushNameSetting:
		// Send presence available when connecting and when the pushname is changed.
		// This makes sure that outgoing messages always have the right pushname.
		err := wa.updatePresence(ctx, types.PresenceUnavailable)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to send presence after push name update")
		}
		_, _, err = wa.GetStore().Contacts.PutPushName(ctx, wa.JID.ToNonAD(), evt.Action.GetName())
		if err != nil {
			log.Err(err).Msg("Failed to update push name in store")
		}
		_, _, err = wa.GetStore().Contacts.PutPushName(ctx, wa.GetStore().GetLID().ToNonAD(), evt.Action.GetName())
		if err != nil {
			log.Err(err).Msg("Failed to update push name in store")
		}
		go wa.syncGhost(wa.JID.ToNonAD(), "push name setting", nil)
	case *events.Contact:
		go wa.syncGhost(evt.JID, "contact event", nil)
	case *events.PushName:
		go wa.syncGhost(evt.JID, "push name event", nil)
	case *events.BusinessName:
		go wa.syncGhost(evt.JID, "business name event", nil)

	case *events.Connected:
		log.Debug().Msg("Connected to WhatsApp socket")
		wa.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
		if len(wa.GetStore().PushName) > 0 {
			go func() {
				err := wa.updatePresence(ctx, types.PresenceUnavailable)
				if err != nil {
					log.Warn().Err(err).Msg("Failed to send initial presence after connecting")
				}
			}()
			go wa.syncRemoteProfile(ctx, nil)
		}
	case *events.OfflineSyncPreview:
		log.Info().
			Int("message_count", evt.Messages).
			Int("receipt_count", evt.Receipts).
			Int("notification_count", evt.Notifications).
			Int("app_data_change_count", evt.AppDataChanges).
			Msg("Server sent number of events that were missed during downtime")
	case *events.OfflineSyncCompleted:
		if !wa.PhoneRecentlySeen(true) {
			log.Info().
				Int("evt_count", evt.Count).
				Time("phone_last_seen", wa.UserLogin.Metadata.(*waid.UserLoginMetadata).PhoneLastSeen.Time).
				Msg("Offline sync completed, but phone last seen date is still old")
		} else {
			log.Info().
				Int("evt_count", evt.Count).
				Msg("Offline sync completed")
		}
		wa.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
		wa.notifyOfflineSyncWaiter(nil)
	case *events.LoggedOut:
		wa.handleWALogout(evt.Reason, evt.OnConnect)
		wa.notifyOfflineSyncWaiter(fmt.Errorf("logged out: %s", evt.Reason))
	case *events.Disconnected:
		// Don't send the normal transient disconnect state if we're already in a different transient disconnect state.
		// TODO remove this if/when the phone offline state is moved to a sub-state of CONNECTED
		if wa.UserLogin.BridgeState.GetPrev().Error != WAPhoneOffline && wa.PhoneRecentlySeen(false) {
			wa.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: WADisconnected})
		}
		wa.notifyOfflineSyncWaiter(fmt.Errorf("disconnected"))
	case *events.StreamError:
		var message string
		if evt.Code != "" {
			message = fmt.Sprintf("Unknown stream error with code %s", evt.Code)
		} else if children := evt.Raw.GetChildren(); len(children) > 0 {
			message = fmt.Sprintf("Unknown stream error (contains %s node)", children[0].Tag)
		} else {
			message = "Unknown stream error"
		}
		wa.UserLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateUnknownError,
			Error:      WAStreamError,
			Message:    message,
		})
		wa.notifyOfflineSyncWaiter(fmt.Errorf("stream error: %s", message))
	case *events.StreamReplaced:
		wa.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateUnknownError, Error: WAStreamReplaced})
		wa.notifyOfflineSyncWaiter(fmt.Errorf("stream replaced"))
	case *events.KeepAliveTimeout:
		wa.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: WAKeepaliveTimeout})
	case *events.KeepAliveRestored:
		log.Info().Msg("Keepalive restored after timeouts, sending connected event")
		wa.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
	case *events.ConnectFailure:
		wa.UserLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateUnknownError,
			Error:      status.BridgeStateErrorCode(fmt.Sprintf("wa-connect-failure-%d", evt.Reason)),
			Message:    fmt.Sprintf("Unknown connection failure: %s (%s)", evt.Reason, evt.Message),
		})
		wa.notifyOfflineSyncWaiter(fmt.Errorf("connection failure: %s (%s)", evt.Reason, evt.Message))
	case *events.ClientOutdated:
		wa.UserLogin.Log.Error().Msg("Got a client outdated connect failure. The bridge is likely out of date, please update immediately.")
		wa.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateUnknownError, Error: WAClientOutdated})
		wa.notifyOfflineSyncWaiter(fmt.Errorf("client outdated"))
	case *events.TemporaryBan:
		wa.UserLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateBadCredentials,
			Error:      WATemporaryBan,
			Message:    evt.String(),
		})
		wa.notifyOfflineSyncWaiter(fmt.Errorf("temporary ban: %s", evt.String()))
	default:
		log.Debug().Type("event_type", rawEvt).Msg("Unhandled WhatsApp event")
	}
	return
}

func (wa *WhatsAppClient) rerouteWAMessage(ctx context.Context, evtType string, info *types.MessageSource, msgID any) {
	if info.Chat.Server == types.HiddenUserServer && info.SenderAlt.IsEmpty() {
		info.SenderAlt, _ = wa.GetStore().LIDs.GetPNForLID(ctx, info.Sender)
	}
	if info.Chat.Server == types.HiddenUserServer && info.Sender.ToNonAD() == info.Chat && info.SenderAlt.Server == types.DefaultUserServer {
		wa.UserLogin.Log.Debug().
			Stringer("lid", info.Sender).
			Stringer("pn", info.SenderAlt).
			Any("message_id", msgID).
			Str("evt_type", evtType).
			Msg("Forced LID DM sender to phone number in incoming message")
		info.Sender, info.SenderAlt = info.SenderAlt, info.Sender
		info.Chat = info.Sender.ToNonAD()
	} else if info.Chat.Server == types.HiddenUserServer && info.IsFromMe && info.RecipientAlt.Server == types.DefaultUserServer {
		wa.UserLogin.Log.Debug().
			Stringer("lid", info.Chat).
			Stringer("pn", info.RecipientAlt).
			Any("message_id", msgID).
			Str("evt_type", evtType).
			Msg("Forced LID DM sender to phone number in own message sent from another device")
		info.Chat = info.RecipientAlt.ToNonAD()
		if info.Sender.Server == types.HiddenUserServer {
			info.Sender, info.SenderAlt = info.SenderAlt, info.Sender
			if info.Sender.IsEmpty() {
				info.Sender = wa.GetStore().GetJID()
				info.Sender.Device = info.SenderAlt.Device
			}
		}
	} else if info.Sender.Server == types.BotServer && info.Chat.Server == types.HiddenUserServer {
		chatPN, err := wa.GetStore().LIDs.GetPNForLID(ctx, info.Chat)
		if err != nil {
			wa.UserLogin.Log.Err(err).
				Any("message_id", msgID).
				Stringer("lid", info.Chat).
				Str("evt_type", evtType).
				Msg("Failed to get phone number of DM for incoming bot message")
		} else if !chatPN.IsEmpty() {
			wa.UserLogin.Log.Debug().
				Stringer("lid", info.Chat).
				Stringer("pn", chatPN).
				Any("message_id", msgID).
				Str("evt_type", evtType).
				Msg("Forced LID chat to phone number in bot message")
			info.Chat = chatPN
		}
	}
}

func (wa *WhatsAppClient) handleWAMessage(ctx context.Context, evt *events.Message) (success bool) {
	success = true
	wa.rerouteWAMessage(ctx, "message", &evt.Info.MessageSource, evt.Info.ID)
	wa.UserLogin.Log.Trace().
		Any("info", evt.Info).
		Any("payload", evt.Message).
		Msg("Received WhatsApp message")
	if evt.Info.Chat == types.StatusBroadcastJID && !wa.Main.Config.EnableStatusBroadcast {
		return
	}
	if evt.Info.IsFromMe &&
		evt.Message.GetProtocolMessage().GetHistorySyncNotification() != nil &&
		wa.Main.Bridge.Config.Backfill.Enabled &&
		wa.Client.ManualHistorySyncDownload {
		wa.saveWAHistorySyncNotification(ctx, evt.Message.ProtocolMessage.HistorySyncNotification)
	}

	messageAssoc := evt.Message.GetMessageContextInfo().GetMessageAssociation()
	if assocType := messageAssoc.GetAssociationType(); assocType == waE2E.MessageAssociation_HD_IMAGE_DUAL_UPLOAD || assocType == waE2E.MessageAssociation_HD_VIDEO_DUAL_UPLOAD {
		parentKey := messageAssoc.GetParentMessageKey()
		associatedMessage := evt.Message.GetAssociatedChildMessage().GetMessage()
		wa.UserLogin.Log.Debug().
			Str("message_id", evt.Info.ID).
			Str("parent_id", parentKey.GetID()).
			Stringer("assoc_type", assocType).
			Msg("Received HD replacement message, converting to edit")

		protocolMsg := &waE2E.ProtocolMessage{
			Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
			Key:           parentKey,
			EditedMessage: associatedMessage,
		}
		evt.Message = &waE2E.Message{
			ProtocolMessage: protocolMsg,
		}
	} else if assocType == waE2E.MessageAssociation_MOTION_PHOTO {
		//evt.Message = evt.Message.GetAssociatedChildMessage().GetMessage()
		wa.UserLogin.Log.Debug().
			Str("message_id", evt.Info.ID).
			Str("parent_id", messageAssoc.GetParentMessageKey().GetID()).
			Msg("Ignoring motion photo update")
		return
	}

	parsedMessageType := getMessageType(evt.Message)
	if parsedMessageType == "ignore" || strings.HasPrefix(parsedMessageType, "unknown_protocol_") {
		return
	}

	// CocoCode Custom Handling
	// CocoCode: Intercept revoke messages - send a notice instead of deleting
	if parsedMessageType == "revoke" {
		return wa.handleWAMessageRevoke(ctx, evt)
	}
	// CocoCode: Intercept edit messages - send a reply with new content instead of replacing
	if parsedMessageType == "edit" {
		return wa.handleWAMessageEdit(ctx, evt)
	}

	if encReact := evt.Message.GetEncReactionMessage(); encReact != nil {
		decrypted, err := wa.Client.DecryptReaction(ctx, evt)
		if err != nil {
			wa.UserLogin.Log.Err(err).Str("message_id", evt.Info.ID).Msg("Failed to decrypt reaction")
			return
		}
		decrypted.Key = encReact.GetTargetMessageKey()
		evt.Message.ReactionMessage = decrypted
	}
	if encComment := evt.Message.GetEncCommentMessage(); encComment != nil {
		decrypted, err := wa.Client.DecryptComment(ctx, evt)
		if err != nil {
			wa.UserLogin.Log.Err(err).Str("message_id", evt.Info.ID).Msg("Failed to decrypt comment")
		} else {
			decrypted.EncCommentMessage = evt.Message.GetEncCommentMessage()
			evt.Message = decrypted
		}
	}
	if encMessage := evt.Message.GetSecretEncryptedMessage(); encMessage != nil {
		decrypted, err := wa.Client.DecryptSecretEncryptedMessage(ctx, evt)
		if err != nil {
			wa.UserLogin.Log.Err(err).Str("message_id", evt.Info.ID).Msg("Failed to decrypt message")
			return
		}
		evt.RawMessage = decrypted
		evt.UnwrapRaw()
		parsedMessageType = getMessageType(evt.Message)
	}
	res := wa.UserLogin.QueueRemoteEvent(&WAMessageEvent{
		MessageInfoWrapper: &MessageInfoWrapper{
			Info: evt.Info,
			wa:   wa,
		},
		Message:  evt.Message,
		MsgEvent: evt,

		parsedMessageType: parsedMessageType,
	})
	return res.Success
}

// handleWAMessageRevoke handles message revoke events by sending a notice
// instead of deleting the message. This preserves the original message and
// adds a reply indicating it was revoked.
func (wa *WhatsAppClient) handleWAMessageRevoke(ctx context.Context, evt *events.Message) bool {
	protocolMsg := evt.Message.GetProtocolMessage()
	if protocolMsg == nil || protocolMsg.GetKey() == nil {
		return true
	}

	revokedKey := protocolMsg.GetKey()
	revokedMsgID := revokedKey.GetID()
	senderJID := types.JID{
		User:   revokedKey.GetRemoteJID(),
		Server: types.DefaultUserServer,
	}
	if revokedKey.GetFromMe() {
		senderJID = evt.Info.Sender
	}

	// Determine who revoked the message
	var revokerName string
	if evt.Info.IsFromMe {
		revokerName = "You"
	} else {
		revokerName = evt.Info.PushName
		if revokerName == "" {
			revokerName = evt.Info.Sender.User
		}
	}

	wa.UserLogin.Log.Info().
		Str("revoked_message_id", revokedMsgID).
		Str("revoker", revokerName).
		Stringer("chat", evt.Info.Chat).
		Msg("Message was revoked - sending notice instead of deleting")

	// Create a notice message that will be sent as a reply
	noticeText := fmt.Sprintf("⚠️ %s revoked a message", revokerName)

	// Queue a simple message event with the revoke notice
	// We use the revoke event's info but send it as a text notice
	return wa.UserLogin.QueueRemoteEvent(&simplevent.Message[*waE2E.Message]{
		EventMeta: simplevent.EventMeta{
			Type: bridgev2.RemoteEventMessage,
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.
					Str("revoked_message_id", revokedMsgID).
					Str("revoker", revokerName)
			},
			PortalKey:    wa.makeWAPortalKey(evt.Info.Chat),
			Sender:       wa.makeEventSender(ctx, evt.Info.Sender),
			CreatePortal: false,
			Timestamp:    evt.Info.Timestamp,
		},
		ID:            networkid.MessageID(evt.Info.ID),
		TargetMessage: waid.MakeMessageID(evt.Info.Chat, senderJID, revokedMsgID),
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data *waE2E.Message) (*bridgev2.ConvertedMessage, error) {
			return &bridgev2.ConvertedMessage{
				Parts: []*bridgev2.ConvertedMessagePart{{
					Type: event.EventMessage,
					Content: &event.MessageEventContent{
						MsgType: event.MsgNotice,
						Body:    noticeText,
					},
				}},
				ReplyTo: &networkid.MessageOptionalPartID{
					MessageID: waid.MakeMessageID(evt.Info.Chat, senderJID, revokedMsgID),
				},
			}, nil
		},
		Data: evt.Message,
	}).Success
}

// handleWAMessageEdit handles message edit events by sending a reply with the
// new content instead of replacing the original message inline. This preserves
// the original message and shows what it was edited to.
func (wa *WhatsAppClient) handleWAMessageEdit(ctx context.Context, evt *events.Message) bool {
	protocolMsg := evt.Message.GetProtocolMessage()
	if protocolMsg == nil || protocolMsg.GetKey() == nil {
		return true
	}

	editedKey := protocolMsg.GetKey()
	editedMsgID := editedKey.GetID()
	editedMessage := protocolMsg.GetEditedMessage()

	senderJID := types.JID{
		User:   editedKey.GetRemoteJID(),
		Server: types.DefaultUserServer,
	}
	if editedKey.GetFromMe() {
		senderJID = evt.Info.Sender
	}

	// Determine who edited the message
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

	wa.UserLogin.Log.Info().
		Str("edited_message_id", editedMsgID).
		Str("editor", editorName).
		Stringer("chat", evt.Info.Chat).
		Str("new_content_preview", truncateString(newContent, 50)).
		Msg("Message was edited - sending notice with new content instead of replacing")

	// Create a notice message that will be sent as a reply
	noticeText := fmt.Sprintf("✏️ %s edited this message:\n\n%s", editorName, newContent)

	// Queue a simple message event with the edit notice
	return wa.UserLogin.QueueRemoteEvent(&simplevent.Message[*waE2E.Message]{
		EventMeta: simplevent.EventMeta{
			Type: bridgev2.RemoteEventMessage,
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.
					Str("edited_message_id", editedMsgID).
					Str("editor", editorName)
			},
			PortalKey:    wa.makeWAPortalKey(evt.Info.Chat),
			Sender:       wa.makeEventSender(ctx, evt.Info.Sender),
			CreatePortal: false,
			Timestamp:    evt.Info.Timestamp,
		},
		ID:            networkid.MessageID(evt.Info.ID),
		TargetMessage: waid.MakeMessageID(evt.Info.Chat, senderJID, editedMsgID),
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data *waE2E.Message) (*bridgev2.ConvertedMessage, error) {
			return &bridgev2.ConvertedMessage{
				Parts: []*bridgev2.ConvertedMessagePart{{
					Type: event.EventMessage,
					Content: &event.MessageEventContent{
						MsgType: event.MsgNotice,
						Body:    noticeText,
					},
				}},
				ReplyTo: &networkid.MessageOptionalPartID{
					MessageID: waid.MakeMessageID(evt.Info.Chat, senderJID, editedMsgID),
				},
			}, nil
		},
		Data: evt.Message,
	}).Success
}

// truncateString truncates a string to maxLen characters and adds "..." if truncated
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func (wa *WhatsAppClient) handleWAUndecryptableMessage(ctx context.Context, evt *events.UndecryptableMessage) bool {
	wa.rerouteWAMessage(ctx, "undecryptable message", &evt.Info.MessageSource, evt.Info.ID)
	wa.UserLogin.Log.Debug().
		Any("info", evt.Info).
		Bool("unavailable", evt.IsUnavailable).
		Str("decrypt_fail", string(evt.DecryptFailMode)).
		Msg("Received undecryptable WhatsApp message")
	wa.trackUndecryptable(evt)
	if evt.DecryptFailMode == events.DecryptFailHide {
		return true
	}
	if evt.Info.Chat == types.StatusBroadcastJID && !wa.Main.Config.EnableStatusBroadcast {
		return true
	}
	res := wa.UserLogin.QueueRemoteEvent(&WAUndecryptableMessage{
		MessageInfoWrapper: &MessageInfoWrapper{
			Info: evt.Info,
			wa:   wa,
		},
		Type: evt.UnavailableType,
	})
	return res.Success
}

func (wa *WhatsAppClient) handleWAReceipt(ctx context.Context, evt *events.Receipt) (success bool) {
	wa.rerouteWAMessage(ctx, "receipt", &evt.MessageSource, evt.MessageIDs)
	if evt.IsFromMe && evt.Sender.Device == 0 {
		wa.phoneSeen(evt.Timestamp)
	}

	// CocoCode Custom Handling
	// CocoCode: Send reaction-based delivery status
	go wa.sendDeliveryReaction(ctx, evt)

	var evtType bridgev2.RemoteEventType
	switch evt.Type {
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf:
		evtType = bridgev2.RemoteEventReadReceipt
	case types.ReceiptTypeDelivered:
		evtType = bridgev2.RemoteEventDeliveryReceipt
	case types.ReceiptTypeSender:
		fallthrough
	default:
		return true
	}
	targets := make([]networkid.MessageID, len(evt.MessageIDs))
	messageSender := wa.JID
	if !evt.MessageSender.IsEmpty() {
		messageSender = evt.MessageSender
	} else if evt.Chat.Server == types.GroupServer && evt.Sender.Server == types.HiddenUserServer {
		lid := wa.GetStore().GetLID()
		if !lid.IsEmpty() {
			messageSender = lid
		}
	}
	for i, id := range evt.MessageIDs {
		targets[i] = waid.MakeMessageID(evt.Chat, messageSender, id)
	}
	res := wa.UserLogin.QueueRemoteEvent(&simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:      evtType,
			PortalKey: wa.makeWAPortalKey(evt.Chat),
			Sender:    wa.makeEventSender(ctx, evt.Sender),
			Timestamp: evt.Timestamp,
		},
		Targets: targets,
	})
	return res.Success
}

// sendDeliveryReaction sends a reaction to indicate message delivery/read status
// ✓ = sent (message appeared), ✓✓ = delivered, 👁️ = read
func (wa *WhatsAppClient) sendDeliveryReaction(ctx context.Context, evt *events.Receipt) {
	log := wa.UserLogin.Log.With().
		Str("action", "send_delivery_reaction").
		Stringer("chat", evt.Chat).
		Str("receipt_type", string(evt.Type)).
		Str("COCO", "check me out").
		Logger()

	log.Warn().Str("Coco", "what is the type now").Str("COCO", "check me out").Msg("CHECKING RECEIPTS")

	// Determine which emoji to use based on receipt type
	var emoji string
	switch evt.Type {
	case types.ReceiptTypeSender:
		emoji = "🙏" // Sent - 1 tick
	case types.ReceiptTypeDelivered:
		emoji = "✅" // Delivered (two ticks)
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf:
		emoji = "👁️" // Read (blue ticks / seen)
	default:
		emoji = "💪"
		// Don't send reactions for other receipt types
		return
	}

	messageSender := wa.JID
	if !evt.MessageSender.IsEmpty() {
		messageSender = evt.MessageSender
	}

	for _, msgID := range evt.MessageIDs {
		targetMsgID := waid.MakeMessageID(evt.Chat, messageSender, msgID)

		wa.UserLogin.QueueRemoteEvent(&simplevent.Reaction{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventReaction,
				PortalKey: wa.makeWAPortalKey(evt.Chat),
				Sender:    wa.makeEventSender(ctx, evt.Sender),
				Timestamp: evt.Timestamp,
			},
			TargetMessage: targetMsgID,
			Emoji:         emoji,
		})

		log.Debug().
			Str("message_id", msgID).
			Str("emoji", emoji).
			Msg("Sent delivery status reaction")
	}
}

func (wa *WhatsAppClient) handleWAChatPresence(ctx context.Context, evt *events.ChatPresence) {
	if evt.Chat.Server == types.HiddenUserServer && evt.Sender.ToNonAD() == evt.Chat {
		if evt.SenderAlt.IsEmpty() {
			evt.SenderAlt, _ = wa.GetStore().LIDs.GetPNForLID(ctx, evt.Sender)
		}
		if evt.SenderAlt.Server == types.DefaultUserServer {
			evt.Sender, evt.SenderAlt = evt.SenderAlt, evt.Sender
			evt.Chat = evt.Sender.ToNonAD()
		}
	}
	typingType := bridgev2.TypingTypeText
	timeout := 15 * time.Second
	if evt.Media == types.ChatPresenceMediaAudio {
		typingType = bridgev2.TypingTypeRecordingMedia
	}
	if evt.State == types.ChatPresencePaused {
		timeout = 0
	}

	wa.UserLogin.QueueRemoteEvent(&simplevent.Typing{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventTyping,
			LogContext: nil,
			PortalKey:  wa.makeWAPortalKey(evt.Chat),
			Sender:     wa.makeEventSender(ctx, evt.Sender),
			Timestamp:  time.Now(),
		},
		Timeout: timeout,
		Type:    typingType,
	})
}

func (wa *WhatsAppClient) handleWALogout(reason events.ConnectFailureReason, onConnect bool) {
	errorCode := WAUnknownLogout
	if reason == events.ConnectFailureLoggedOut {
		errorCode = WALoggedOut
	} else if reason == events.ConnectFailureMainDeviceGone {
		errorCode = WAMainDeviceGone
	}
	wa.Client.Disconnect()
	wa.Client = nil
	wa.JID = types.EmptyJID
	wa.UserLogin.Metadata.(*waid.UserLoginMetadata).WADeviceID = 0
	wa.UserLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      errorCode,
	})
}

const callEventMaxAge = 15 * time.Minute

func (wa *WhatsAppClient) handleWACallStart(ctx context.Context, group, sender, senderAlt types.JID, id, callType string, ts time.Time) bool {
	if !wa.Main.Config.CallStartNotices || time.Since(ts) > callEventMaxAge {
		return true
	}
	if sender.Server == types.HiddenUserServer && senderAlt.Server == types.DefaultUserServer {
		wa.UserLogin.Log.Debug().
			Stringer("lid", sender).
			Stringer("pn", senderAlt).
			Str("call_id", id).
			Msg("Forced LID caller to phone number in incoming call")
		sender, senderAlt = senderAlt, sender
	}
	chat := group
	if chat.IsEmpty() {
		chat = sender
	}
	return wa.UserLogin.QueueRemoteEvent(&simplevent.Message[string]{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventMessage,
			LogContext:   nil,
			PortalKey:    wa.makeWAPortalKey(chat),
			Sender:       wa.makeEventSender(ctx, sender),
			CreatePortal: true,
			Timestamp:    ts,
		},
		Data:               callType,
		ID:                 waid.MakeFakeMessageID(chat, sender, "call-"+id),
		ConvertMessageFunc: convertCallStart,
	}).Success
}

func convertCallStart(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, callType string) (*bridgev2.ConvertedMessage, error) {
	text := "Incoming call. Use the WhatsApp app to answer."
	if callType != "" {
		text = fmt.Sprintf("Incoming %s call. Use the WhatsApp app to answer.", callType)
	}
	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    text,
			},
		}},
	}, nil
}

func (wa *WhatsAppClient) handleWAIdentityChange(ctx context.Context, evt *events.IdentityChange) {
	if !wa.Main.Config.IdentityChangeNotices {
		return
	}
	wa.UserLogin.QueueRemoteEvent(&simplevent.Message[*events.IdentityChange]{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventMessage,
			LogContext:   nil,
			PortalKey:    wa.makeWAPortalKey(evt.JID),
			Sender:       wa.makeEventSender(ctx, evt.JID),
			CreatePortal: false,
			Timestamp:    evt.Timestamp,
		},
		Data:               evt,
		ID:                 waid.MakeFakeMessageID(evt.JID, evt.JID, "idchange-"+strconv.FormatInt(evt.Timestamp.UnixMilli(), 10)),
		ConvertMessageFunc: convertIdentityChange,
	})
}

func convertIdentityChange(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data *events.IdentityChange) (*bridgev2.ConvertedMessage, error) {
	ghost, err := portal.Bridge.GetGhostByID(ctx, waid.MakeUserID(data.JID))
	if err != nil {
		return nil, err
	}
	text := fmt.Sprintf("Your security code with %s changed.", ghost.Name)
	if data.Implicit {
		text = fmt.Sprintf("Your security code with %s (device #%d) changed.", ghost.Name, data.JID.Device)
	}
	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgNotice,
				Body:    text,
			},
		}},
	}, nil
}

func (wa *WhatsAppClient) handleWADeleteChat(evt *events.DeleteChat) bool {
	return wa.UserLogin.QueueRemoteEvent(&simplevent.ChatDelete{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatDelete,
			PortalKey: wa.makeWAPortalKey(evt.JID),
			Timestamp: evt.Timestamp,
		},
		OnlyForMe: true,
		Children:  true,
	}).Success
}

func (wa *WhatsAppClient) handleWADeleteForMe(evt *events.DeleteForMe) bool {
	return wa.UserLogin.QueueRemoteEvent(&simplevent.MessageRemove{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventMessageRemove,
			PortalKey: wa.makeWAPortalKey(evt.ChatJID),
			Timestamp: evt.Timestamp,
		},
		TargetMessage: waid.MakeMessageID(evt.ChatJID, evt.SenderJID, evt.MessageID),
		OnlyForMe:     true,
	}).Success
}

func (wa *WhatsAppClient) handleWAMarkChatAsRead(ctx context.Context, evt *events.MarkChatAsRead) bool {
	return wa.UserLogin.QueueRemoteEvent(&simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventReadReceipt,
			PortalKey: wa.makeWAPortalKey(evt.JID),
			Sender:    wa.makeEventSender(ctx, wa.JID),
			Timestamp: evt.Timestamp,
		},
		ReadUpTo: evt.Timestamp,
	}).Success
}

func (wa *WhatsAppClient) syncGhost(jid types.JID, reason string, pictureID *string) {
	log := wa.UserLogin.Log.With().
		Str("action", "sync ghost").
		Str("reason", reason).
		Str("picture_id", ptr.Val(pictureID)).
		Stringer("jid", jid).
		Logger()
	ctx := log.WithContext(wa.Main.Bridge.BackgroundCtx)
	ghost, err := wa.Main.Bridge.GetGhostByID(ctx, waid.MakeUserID(jid))
	if err != nil {
		log.Err(err).Msg("Failed to get ghost")
		return
	}
	if pictureID != nil && *pictureID != "" && ghost.AvatarID == networkid.AvatarID(*pictureID) {
		return
	}
	userInfo, err := wa.getUserInfo(ctx, jid, pictureID != nil)
	if err != nil {
		log.Err(err).Msg("Failed to get user info")
	} else {
		ghost.UpdateInfo(ctx, userInfo)
		log.Debug().Msg("Synced ghost info")
		wa.syncAltGhostWithInfo(ctx, jid, userInfo)
	}
	go wa.syncRemoteProfile(ctx, ghost)
}

func (wa *WhatsAppClient) handleWAPictureUpdate(ctx context.Context, evt *events.Picture) bool {
	if evt.JID.Server == types.DefaultUserServer || evt.JID.Server == types.HiddenUserServer || evt.JID.Server == types.BotServer {
		go wa.syncGhost(evt.JID, "picture event", &evt.PictureID)

		// CocoCode Custom Handling
		// CocoCode: Also send the profile picture as a message to the chat
		go wa.sendPictureUpdateNotice(ctx, evt)

		return true
	} else {
		var changes bridgev2.ChatInfo
		if evt.Remove {
			changes.Avatar = &bridgev2.Avatar{Remove: true, ID: "remove"}
		} else {
			changes.ExtraUpdates = wa.makePortalAvatarFetcher(evt.PictureID, evt.Author, evt.Timestamp)
		}
		return wa.UserLogin.QueueRemoteEvent(&simplevent.ChatInfoChange{
			EventMeta: simplevent.EventMeta{
				Type: bridgev2.RemoteEventChatInfoChange,
				LogContext: func(c zerolog.Context) zerolog.Context {
					return c.
						Str("wa_event_type", "picture").
						Stringer("picture_author", evt.Author).
						Str("new_picture_id", evt.PictureID).
						Bool("remove_picture", evt.Remove)
				},
				PortalKey: wa.makeWAPortalKey(evt.JID),
				Sender:    wa.makeEventSender(ctx, evt.Author),
				Timestamp: evt.Timestamp,
			},
			ChatInfoChange: &bridgev2.ChatInfoChange{
				ChatInfo: &changes,
			},
		}).Success
	}
}

// CocoCode: sendPictureUpdateNotice sends the profile picture as a message to the chat
// so you can see what the picture was changed to (or that it was removed)
func (wa *WhatsAppClient) sendPictureUpdateNotice(ctx context.Context, evt *events.Picture) {
	log := wa.UserLogin.Log.With().
		Str("action", "send_picture_update_notice").
		Stringer("jid", evt.JID).
		Str("picture_id", evt.PictureID).
		Bool("removed", evt.Remove).
		Logger()

	// CocoCode: Normalize JID to phone number for portal lookup
	// If this is a LID, convert it to the phone number JID so we use the correct portal
	portalJID := evt.JID
	if evt.JID.Server == types.HiddenUserServer {
		pn, err := wa.GetStore().LIDs.GetPNForLID(ctx, evt.JID)
		if err != nil {
			log.Err(err).Msg("Failed to get phone number for LID in picture notice")
			// Fall back to using the LID
		} else if !pn.IsEmpty() {
			portalJID = pn
			log.Debug().
				Stringer("original_jid", evt.JID).
				Stringer("normalized_jid", portalJID).
				Msg("Normalized LID to phone number for portal lookup")
		}
	}

	// Get the contact name
	userInfo, _ := wa.getUserInfo(ctx, evt.JID, false)
	contactName := userInfo.Name

	if evt.Remove {
		// Picture was removed - send a text notice
		noticeText := fmt.Sprintf("📷 %s removed their profile picture", contactName)

		wa.UserLogin.QueueRemoteEvent(&simplevent.Message[any]{
			EventMeta: simplevent.EventMeta{
				Type: bridgev2.RemoteEventMessage,
				LogContext: func(c zerolog.Context) zerolog.Context {
					return c.Str("picture_notice", "removed")
				},
				PortalKey:    wa.makeWAPortalKey(portalJID), // CocoCode: Use normalized JID
				Sender:       wa.makeEventSender(ctx, evt.JID),
				CreatePortal: false, // CocoCode: Don't create new portal, should exist
				Timestamp:    evt.Timestamp,
			},
			ID: networkid.MessageID(fmt.Sprintf("picture_remove_%s_%d", evt.JID.String(), evt.Timestamp.Unix())),
			ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data any) (*bridgev2.ConvertedMessage, error) {
				return &bridgev2.ConvertedMessage{
					Parts: []*bridgev2.ConvertedMessagePart{{
						Type: event.EventMessage,
						Content: &event.MessageEventContent{
							MsgType: event.MsgNotice,
							Body:    noticeText,
						},
					}},
				}, nil
			},
		})
		log.Info().Msg("Sent profile picture removal notice")
	} else {
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
		pictureBytes, err := wa.downloadProfilePicture(ctx, pictureInfo.URL)
		if err != nil {
			log.Err(err).Msg("Failed to download profile picture")
			return
		}

		noticeText := fmt.Sprintf("📷 %s updated their profile picture", contactName) // CocoCode: Removed debug text

		wa.UserLogin.QueueRemoteEvent(&simplevent.Message[[]byte]{
			EventMeta: simplevent.EventMeta{
				Type: bridgev2.RemoteEventMessage,
				LogContext: func(c zerolog.Context) zerolog.Context {
					return c.Str("picture_notice", "updated")
				},
				PortalKey:    wa.makeWAPortalKey(portalJID), // CocoCode: Use normalized JID
				Sender:       wa.makeEventSender(ctx, evt.JID),
				CreatePortal: false, // CocoCode: Don't create new portal, should exist
				Timestamp:    evt.Timestamp,
			},
			ID: networkid.MessageID(fmt.Sprintf("picture_update_%s_%d", evt.JID.String(), evt.Timestamp.Unix())),
			ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data []byte) (*bridgev2.ConvertedMessage, error) {
				// Upload the image to Matrix
				contentUri, _, err := intent.UploadMedia(ctx, "", data, "profile_picture.jpg", "image/jpeg")
				if err != nil {
					return nil, fmt.Errorf("failed to upload profile picture: %w", err)
				}

				return &bridgev2.ConvertedMessage{
					Parts: []*bridgev2.ConvertedMessagePart{{
						Type: event.EventMessage,
						Content: &event.MessageEventContent{
							MsgType: event.MsgImage,
							Body:    noticeText,
							URL:     contentUri,
							Info: &event.FileInfo{
								MimeType: "image/jpeg",
								Size:     len(data),
							},
						},
					}},
				}, nil
			},
			Data: pictureBytes,
		})
		log.Info().Msg("Sent profile picture update with image")
	}
}

// downloadProfilePicture downloads a profile picture from a URL
func (wa *WhatsAppClient) downloadProfilePicture(ctx context.Context, url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (wa *WhatsAppClient) handleWAGroupInfoChange(ctx context.Context, evt *events.GroupInfo) bool {
	eventMeta := simplevent.EventMeta{
		Type:         bridgev2.RemoteEventChatInfoChange,
		LogContext:   nil,
		PortalKey:    wa.makeWAPortalKey(evt.JID),
		CreatePortal: true,
		Timestamp:    evt.Timestamp,
	}
	if evt.Sender != nil {
		eventMeta.Sender = wa.makeEventSender(ctx, *evt.Sender)
	}
	if evt.Delete != nil {
		eventMeta.Type = bridgev2.RemoteEventChatDelete
		return wa.UserLogin.QueueRemoteEvent(&simplevent.ChatDelete{EventMeta: eventMeta}).Success
	} else {
		return wa.UserLogin.QueueRemoteEvent(&simplevent.ChatInfoChange{
			EventMeta:      eventMeta,
			ChatInfoChange: wa.wrapGroupInfoChange(ctx, evt),
		}).Success
	}
}

func (wa *WhatsAppClient) handleWAJoinedGroup(ctx context.Context, evt *events.JoinedGroup) bool {
	if wa.createDedup.Pop(evt.CreateKey) {
		return true
	}
	return wa.UserLogin.QueueRemoteEvent(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			LogContext:   nil,
			PortalKey:    wa.makeWAPortalKey(evt.JID),
			CreatePortal: true,
		},
		ChatInfo: wa.wrapGroupInfo(ctx, &evt.GroupInfo),
	}).Success
}

func (wa *WhatsAppClient) handleWANewsletterJoin(ctx context.Context, evt *events.NewsletterJoin) bool {
	return wa.UserLogin.QueueRemoteEvent(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			LogContext:   nil,
			PortalKey:    wa.makeWAPortalKey(evt.ID),
			CreatePortal: true,
		},
		ChatInfo: wa.wrapNewsletterInfo(ctx, &evt.NewsletterMetadata),
	}).Success
}

func (wa *WhatsAppClient) handleWANewsletterLeave(evt *events.NewsletterLeave) bool {
	return wa.UserLogin.QueueRemoteEvent(&simplevent.ChatDelete{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventChatDelete,
			LogContext: nil,
			PortalKey:  wa.makeWAPortalKey(evt.ID),
		},
		OnlyForMe: true,
	}).Success
}

func (wa *WhatsAppClient) handleWAUserLocalPortalInfo(chatJID types.JID, ts time.Time, info *bridgev2.UserLocalPortalInfo) bool {
	return wa.UserLogin.QueueRemoteEvent(&simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatInfoChange,
			PortalKey: wa.makeWAPortalKey(chatJID),
			Timestamp: ts,
		},
		ChatInfoChange: &bridgev2.ChatInfoChange{
			ChatInfo: &bridgev2.ChatInfo{
				UserLocal: info,
			},
		},
	}).Success
}

func (wa *WhatsAppClient) handleWAMute(evt *events.Mute) bool {
	var mutedUntil time.Time
	if evt.Action.GetMuted() {
		mutedUntil = event.MutedForever
		if evt.Action.GetMuteEndTimestamp() > 0 {
			mutedUntil = time.Unix(evt.Action.GetMuteEndTimestamp(), 0)
		}
	} else {
		mutedUntil = bridgev2.Unmuted
	}
	return wa.handleWAUserLocalPortalInfo(evt.JID, evt.Timestamp, &bridgev2.UserLocalPortalInfo{
		MutedUntil: &mutedUntil,
	})
}

func (wa *WhatsAppClient) handleWAArchive(evt *events.Archive) bool {
	var tag event.RoomTag
	if evt.Action.GetArchived() {
		tag = wa.Main.Config.ArchiveTag
	}
	return wa.handleWAUserLocalPortalInfo(evt.JID, evt.Timestamp, &bridgev2.UserLocalPortalInfo{
		Tag: &tag,
	})
}

func (wa *WhatsAppClient) handleWAPin(evt *events.Pin) bool {
	var tag event.RoomTag
	if evt.Action.GetPinned() {
		tag = wa.Main.Config.PinnedTag
	}
	return wa.handleWAUserLocalPortalInfo(evt.JID, evt.Timestamp, &bridgev2.UserLocalPortalInfo{
		Tag: &tag,
	})
}
