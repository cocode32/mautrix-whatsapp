package connector

import (
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/commands"
)

// Command to fetch and display the current profile picture of a contact
var cmdPicture = &commands.FullHandler{
	Func: fnPicture,
	Name: "picture",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionGeneral,
		Description: "Fetch and display the current profile picture of the contact in this chat.",
	},
	RequiresLogin:  true,
	RequiresPortal: true,
}

// Function for the command
func fnPicture(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}

	portalJID, err := waid.ParsePortalID(ce.Portal.ID)
	if err != nil {
		ce.Reply("Failed to parse portal ID: %v", err)
		return
	}

	// Check if this is a private chat (not a group)
	if portalJID.Server != types.DefaultUserServer && portalJID.Server != types.HiddenUserServer {
		ce.Reply("This command only works in private chats, not groups.")
		return
	}

	wa := login.Client.(*WhatsAppClient)

	// Use the existing helper function to send the current profile picture
	go wa.sendMatrixCurrentProfilePicture(ce.Ctx, portalJID, "manual_command")

	ce.React("📷")
}
