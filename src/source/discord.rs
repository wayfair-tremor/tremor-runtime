// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#![cfg(not(tarpaulin_include))]

use std::{
    collections::HashMap as StdHashMap,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::{codec::Codec, source::prelude::*, QSIZE};
use async_channel::{Receiver, Sender, TryRecvError};
use halfbrown::HashMap;
use serde::Serialize;
use serenity::{
    client::bridge::gateway::GatewayIntents,
    model::{
        channel::{Channel, ChannelCategory, GuildChannel, Message, Reaction, ReactionType},
        event::{
            ChannelPinsUpdateEvent, GuildMembersChunkEvent, ResumedEvent, TypingStartEvent,
            VoiceServerUpdateEvent,
        },
        guild::{Emoji, Guild, GuildUnavailable, Member, PartialGuild, Role},
        id::{ChannelId, EmojiId, GuildId, MessageId, RoleId, UserId},
        prelude::{CurrentUser, Presence, Ready, User, VoiceState},
    },
    prelude::*,
};
use tremor_script::prelude::*;
use tremor_value::to_value;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub token: String,
    #[serde(default)]
    pub intents: Vec<Intents>,
}

impl ConfigImpl for Config {}

#[derive(Clone)]
pub struct Discord {
    pub config: Config,
    origin_uri: EventOriginUri,
    onramp_id: TremorUrl,
    client: Option<(Sender<Value<'static>>, Receiver<Value<'static>>)>,
}
impl std::fmt::Debug for Discord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Discord")
    }
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug)]
pub enum Intents {
    /// Guilds Grouping
    Guilds,
    /// GuildMembers Grouping
    GuildMembers,
    /// GuildBans Grouping
    GuildBans,
    /// GuildEmojis Grouping
    GuildEmojis,
    /// GuildIntegrations Grouping
    GuildIntegrations,
    /// GuildWebHooks Grouping
    GuildWebHooks,
    /// GuildInvites Grouping
    GuildInvites,
    /// GuildVoiceStates
    GuildVoiceStates,
    /// GuildPresence
    GuildPresence,
    /// GuildMessages
    GuildMessages,
    /// GuildMessageReactions
    GuildMessageReactions,
    /// GuildMessageTyping
    GuildMessageTyping,
    /// DirectMessages
    DirectMessages,
    /// DirectMessageReactions
    DirectMessageReactions,
    /// DirectMessageTyping
    DirectMessageTyping,
    /// All intents
    All,
    /// All non privileged details
    NonPrivileged,
}

impl From<Intents> for GatewayIntents {
    fn from(intent: Intents) -> GatewayIntents {
        match intent {
            Intents::Guilds => GatewayIntents::GUILDS,
            Intents::GuildMembers => GatewayIntents::GUILD_MEMBERS,
            Intents::GuildBans => GatewayIntents::GUILD_BANS,
            Intents::GuildEmojis => GatewayIntents::GUILD_EMOJIS,
            Intents::GuildIntegrations => GatewayIntents::GUILD_INTEGRATIONS,
            Intents::GuildWebHooks => GatewayIntents::GUILD_WEBHOOKS,
            Intents::GuildInvites => GatewayIntents::GUILD_INVITES,
            Intents::GuildVoiceStates => GatewayIntents::GUILD_VOICE_STATES,
            Intents::GuildPresence => GatewayIntents::GUILD_PRESENCES,
            Intents::GuildMessages => GatewayIntents::GUILD_MESSAGES,
            Intents::GuildMessageReactions => GatewayIntents::GUILD_MESSAGE_REACTIONS,
            Intents::GuildMessageTyping => GatewayIntents::GUILD_MESSAGE_TYPING,
            Intents::DirectMessages => GatewayIntents::DIRECT_MESSAGES,
            Intents::DirectMessageReactions => GatewayIntents::DIRECT_MESSAGE_REACTIONS,
            Intents::DirectMessageTyping => GatewayIntents::DIRECT_MESSAGE_TYPING,
            Intents::All => GatewayIntents::all(),
            Intents::NonPrivileged => GatewayIntents::non_privileged(),
        }
    }
}
pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let origin_uri = EventOriginUri {
                uid: 0,
                scheme: "tremor-discord".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            };

            Ok(Box::new(Discord {
                origin_uri,
                config,
                onramp_id: id.clone(),
                client: None,
            }))
        } else {
            Err("Missing config for discord onramp".into())
        }
    }
}

struct Handler {
    tx: Sender<Value<'static>>,
    rx: Receiver<Value<'static>>,
    is_loop_running: AtomicBool,
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize)]
enum DiscordMessage {
    AddReaction(Reaction),
    RemoveReaction(Reaction),
    ChannelCreate(GuildChannel),
    ChannelDelete(GuildChannel),
    CategoryCreate(ChannelCategory),
    CategoryDelete(ChannelCategory),
    ChannelUpdate {
        old: Option<Channel>,
        new: Channel,
    },
    BanAddition {
        guild_id: GuildId,
        user: User,
    },
    BanRemoval {
        guild_id: GuildId,
        user: User,
    },
    GuildCreate {
        guild: Guild,
        is_new: bool,
    },
    GuildDelete {
        incomplete: GuildUnavailable,
        full: Option<Guild>,
    },
    EmojiUpdate {
        guild_id: GuildId,
        current_state: StdHashMap<EmojiId, Emoji>,
    },
    IntegrationsUpdate(GuildId),
    MemberAddition {
        guild_id: GuildId,
        new_member: Member,
    },
    MemberRemoval {
        guild_id: GuildId,
        user: User,
        member_data_if_available: Option<Member>,
    },
    MemberUpdate {
        old_if_available: Option<Member>,
        new: Member,
    },
    RoleCreate {
        guild_id: GuildId,
        new: Role,
    },
    RoleDelete {
        guild_id: GuildId,
        removed_role_id: RoleId,
        removed_role_data_if_available: Option<Role>,
    },
    RoleUpdate {
        guild_id: GuildId,
        old_data_if_available: Option<Role>,
        new: Role,
    },
    GuildUpdate {
        old_data_if_available: Option<Guild>,
        new_but_incomplete: PartialGuild,
    },
    MessageDelete {
        channel_id: ChannelId,
        deleted_message_id: MessageId,
        guild_id: Option<GuildId>,
    },
    MessageDeleteBulk {
        channel_id: ChannelId,
        multiple_deleted_messages_ids: Vec<MessageId>,
        guild_id: Option<GuildId>,
    },
    MessageUpdate {
        old_if_available: Option<Message>,
        new: Option<Message>,
        event: serenity::model::event::MessageUpdateEvent,
    },
    ReactionRemoveAll {
        channel_id: ChannelId,
        removed_from_message_id: MessageId,
    },
    UserUpdate {
        old_data: CurrentUser,
        new: CurrentUser,
    },
    VoiceUpdate {
        old: Option<VoiceState>,
        new: VoiceState,
    },
    WebhookUpdate {
        guild_id: GuildId,
        belongs_to_channel_id: ChannelId,
    },
    VoiceServerUpdate(VoiceServerUpdateEvent),
    Resume(ResumedEvent),
    GuildUnavailable(GuildId),
    GuildMembersChunk(GuildMembersChunkEvent),
    TypingStart(TypingStartEvent),
    PresenceReplace(Vec<Presence>),
}

fn to_reaction(v: &Value) -> Option<ReactionType> {
    v.as_char().map_or_else(
        || {
            v.get_u64("id").map(|id| ReactionType::Custom {
                id: EmojiId(id),
                animated: v.get_bool("animated").unwrap_or_default(),
                name: v.get_str("name").map(ToString::to_string),
            })
        },
        |c| Some(ReactionType::Unicode(c.to_string())),
    )
}
fn to_reactions(v: &Value) -> Option<Vec<ReactionType>> {
    v.as_array()
        .map(|a| a.iter().filter_map(to_reaction).collect::<Vec<_>>())
}

#[allow(clippy::too_many_lines)]
async fn reply_loop(rx: Receiver<Value<'static>>, ctx: Context) {
    while let Ok(reply) = rx.recv().await {
        if let Some(reply) = reply.get("guild") {
            let guild = if let Some(id) = reply.get_u64("id") {
                GuildId(id)
            } else {
                continue;
            };

            if let Some(member) = reply.get("member") {
                if let Some(id) = member.get_u64("id") {
                    let user = UserId(id);
                    let mut current_member = match guild.member(&ctx, user).await {
                        Ok(current_member) => current_member,
                        Err(e) => {
                            error!("Member error: {}", e);
                            continue;
                        }
                    };
                    if let Some(to_remove) = member.get_array("remove_roles") {
                        let to_remove: Vec<_> = to_remove
                            .iter()
                            .filter_map(|v| v.as_u64().map(RoleId))
                            .collect();
                        if let Err(e) = current_member.remove_roles(&ctx, &to_remove).await {
                            error!("Role removal error: {}", e);
                        };
                    }

                    if let Some(to_roles) = member.get_array("add_roles") {
                        let to_roles: Vec<_> = to_roles
                            .iter()
                            .filter_map(|v| v.as_u64().map(RoleId))
                            .collect();
                        if let Err(e) = current_member.add_roles(&ctx, &to_roles).await {
                            error!("Role add error: {}", e);
                        };
                    }
                    let r = guild
                        .edit_member(&ctx, id, |m| {
                            if let Some(deafen) = member.get_bool("deafen") {
                                m.deafen(deafen);
                            }
                            if let Some(mute) = member.get_bool("mute") {
                                m.mute(mute);
                            }

                            m
                        })
                        .await;
                    if let Err(e) = r {
                        error!("Mute/Deafen error: {}", e);
                    };
                }
            }
        }
        if let Some(reply) = reply.get("message") {
            let channel = if let Some(id) = reply.get_u64("channel_id") {
                ChannelId(id)
            } else {
                continue;
            };

            if let Some(reply) = reply.get("update") {
                if let Some(message_id) = reply.get_u64("message_id") {
                    let message = match channel.message(&ctx, message_id).await {
                        Ok(message) => message,
                        Err(e) => {
                            error!("Message error: {}", e);
                            continue;
                        }
                    };

                    if let Some(reactions) = reply.get("add_reactions").and_then(to_reactions) {
                        for r in reactions {
                            if let Err(e) = message.react(&ctx, r).await {
                                error!("Message reaction error: {}", e);
                            };
                        }
                    }
                }
            }

            if let Some(reply) = reply.get("send") {
                if let Err(e) = channel
                    .send_message(&ctx, |m| {
                        // Normal content
                        if let Some(content) = reply.get_str("content") {
                            m.content(content);
                        };
                        // Reference to another message
                        if let Some(reference_message) = reply.get_u64("reference_message") {
                            let reference_channel = reply
                                .get_u64("reference_channel")
                                .map_or(channel, ChannelId);
                            m.reference_message((reference_channel, MessageId(reference_message)));
                        };

                        if let Some(tts) = reply.get_bool("tts") {
                            m.tts(tts);
                        };

                        if let Some(embed) = reply.get("embed") {
                            m.embed(|e| {
                                if let Some(author) = embed.get("author") {
                                    e.author(|a| {
                                        if let Some(icon_url) = author.get_str("icon_url") {
                                            a.icon_url(icon_url);
                                        };
                                        if let Some(name) = author.get_str("name") {
                                            a.name(name);
                                        };
                                        if let Some(url) = author.get_str("url") {
                                            a.url(url);
                                        };

                                        a
                                    });
                                };

                                if let Some(colour) = embed.get_u64("colour") {
                                    e.colour(colour);
                                };
                                if let Some(description) = embed.get_str("description") {
                                    e.description(description);
                                };

                                if let Some(fields) = embed.get_array("fields") {
                                    e.fields(fields.iter().filter_map(|v| {
                                        let name = v.get_str("name")?;
                                        let value = v.get_str("value")?;
                                        let inline = v.get_bool("inline").unwrap_or_default();
                                        Some((name, value, inline))
                                    }));
                                };
                                if let Some(footer) = embed.get("footer") {
                                    e.footer(|f| {
                                        if let Some(text) = footer.as_str() {
                                            f.text(text);
                                        };
                                        if let Some(text) = footer.get_str("text") {
                                            f.text(text);
                                        };
                                        if let Some(icon_url) = footer.get_str("icon_url") {
                                            f.icon_url(icon_url);
                                        };

                                        f
                                    });
                                };

                                e
                            });
                        };

                        if let Some(reactions) = reply.get("reactions").and_then(to_reactions) {
                            m.reactions(reactions);
                        };

                        m
                    })
                    .await
                {
                    error!("Discord send error: {}", e)
                };
            };
        }
    }
}

impl Handler {
    async fn forward<T>(&self, msg: T)
    where
        T: Serialize,
    {
        let event: Value<'static> = match to_value(msg) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to decode event: {}", e);
                return;
            }
        };
        if let Err(e) = self.tx.send(event).await {
            error!("Failed to forward event: {}", e)
        }
    }
}
#[async_trait::async_trait]
impl EventHandler for Handler {
    // We use the cache_ready event just in case some cache operation is required in whatever use
    // case you have for this.
    async fn cache_ready(&self, ctx: Context, _guilds: Vec<GuildId>) {
        println!("Cache built successfully!");

        if !self.is_loop_running.load(Ordering::Relaxed) {
            // We have to clone the Arc, as it gets moved into the new thread.
            // tokio::spawn creates a new green thread that can run in parallel with the rest of
            // the application.
            let rx = self.rx.clone();
            task::spawn(async move { reply_loop(rx, ctx).await });

            // Now that the loop is running, we set the bool to true
            self.is_loop_running.swap(true, Ordering::Relaxed);
        }
    }

    async fn channel_create(&self, _ctx: Context, channel: &GuildChannel) {
        self.forward(DiscordMessage::ChannelCreate(channel.clone()))
            .await
    }

    async fn category_create(&self, _ctx: Context, category: &ChannelCategory) {
        self.forward(DiscordMessage::CategoryCreate(category.clone()))
            .await
    }

    async fn category_delete(&self, _ctx: Context, category: &ChannelCategory) {
        self.forward(DiscordMessage::CategoryDelete(category.clone()))
            .await
    }

    async fn channel_delete(&self, _ctx: Context, channel: &GuildChannel) {
        self.forward(DiscordMessage::ChannelDelete(channel.clone()))
            .await
    }

    async fn channel_pins_update(&self, _ctx: Context, pin: ChannelPinsUpdateEvent) {
        self.forward(pin).await
    }

    async fn invite_create(&self, _ctx: Context, data: serenity::model::event::InviteCreateEvent) {
        self.forward(data).await
    }

    async fn invite_delete(&self, _ctx: Context, data: serenity::model::event::InviteDeleteEvent) {
        self.forward(data).await
    }

    async fn message(&self, _ctx: Context, msg: Message) {
        self.forward(msg).await
    }

    async fn reaction_add(&self, _ctx: Context, add_reaction: Reaction) {
        self.forward(DiscordMessage::AddReaction(add_reaction))
            .await
    }

    async fn reaction_remove(&self, _ctx: Context, removed_reaction: Reaction) {
        self.forward(DiscordMessage::RemoveReaction(removed_reaction))
            .await
    }

    async fn presence_replace(&self, _ctx: Context, p: Vec<Presence>) {
        self.forward(DiscordMessage::PresenceReplace(p)).await
    }

    async fn presence_update(
        &self,
        _ctx: Context,
        new_data: serenity::model::event::PresenceUpdateEvent,
    ) {
        self.forward(new_data).await
    }

    async fn ready(&self, _: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
    }

    async fn typing_start(&self, _ctx: Context, e: TypingStartEvent) {
        self.forward(DiscordMessage::TypingStart(e)).await
    }

    async fn channel_update(&self, _ctx: Context, old: Option<Channel>, new: Channel) {
        self.forward(DiscordMessage::ChannelUpdate { old, new })
            .await
    }

    async fn guild_ban_addition(&self, _ctx: Context, guild_id: GuildId, user: User) {
        self.forward(DiscordMessage::BanAddition { guild_id, user })
            .await
    }

    async fn guild_ban_removal(&self, _ctx: Context, guild_id: GuildId, user: User) {
        self.forward(DiscordMessage::BanRemoval { guild_id, user })
            .await
    }

    async fn guild_create(&self, _ctx: Context, guild: Guild, is_new: bool) {
        self.forward(DiscordMessage::GuildCreate { guild, is_new })
            .await
    }

    async fn guild_delete(&self, _ctx: Context, incomplete: GuildUnavailable, full: Option<Guild>) {
        self.forward(DiscordMessage::GuildDelete { incomplete, full })
            .await
    }

    async fn guild_emojis_update(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        current_state: StdHashMap<EmojiId, Emoji>,
    ) {
        self.forward(DiscordMessage::EmojiUpdate {
            guild_id,
            current_state,
        })
        .await
    }

    async fn guild_integrations_update(&self, _ctx: Context, guild_id: GuildId) {
        self.forward(DiscordMessage::IntegrationsUpdate(guild_id))
            .await
    }

    async fn guild_member_addition(&self, _ctx: Context, guild_id: GuildId, new_member: Member) {
        self.forward(DiscordMessage::MemberAddition {
            guild_id,
            new_member,
        })
        .await
    }

    async fn guild_member_removal(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        user: User,
        member_data_if_available: Option<Member>,
    ) {
        self.forward(DiscordMessage::MemberRemoval {
            guild_id,
            user,
            member_data_if_available,
        })
        .await
    }

    async fn guild_member_update(
        &self,
        _ctx: Context,
        old_if_available: Option<Member>,
        new: Member,
    ) {
        self.forward(DiscordMessage::MemberUpdate {
            old_if_available,
            new,
        })
        .await
    }

    async fn guild_members_chunk(&self, _ctx: Context, chunk: GuildMembersChunkEvent) {
        self.forward(DiscordMessage::GuildMembersChunk(chunk)).await
    }

    async fn guild_role_create(&self, _ctx: Context, guild_id: GuildId, new: Role) {
        self.forward(DiscordMessage::RoleCreate { guild_id, new })
            .await
    }

    async fn guild_role_delete(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        removed_role_id: RoleId,
        removed_role_data_if_available: Option<Role>,
    ) {
        self.forward(DiscordMessage::RoleDelete {
            guild_id,
            removed_role_id,
            removed_role_data_if_available,
        })
        .await
    }

    async fn guild_role_update(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        old_data_if_available: Option<Role>,
        new: Role,
    ) {
        self.forward(DiscordMessage::RoleUpdate {
            guild_id,
            old_data_if_available,
            new,
        })
        .await
    }

    async fn guild_unavailable(&self, _ctx: Context, guild_id: GuildId) {
        self.forward(DiscordMessage::GuildUnavailable(guild_id))
            .await
    }

    async fn guild_update(
        &self,
        _ctx: Context,
        old_data_if_available: Option<Guild>,
        new_but_incomplete: PartialGuild,
    ) {
        self.forward(DiscordMessage::GuildUpdate {
            old_data_if_available,
            new_but_incomplete,
        })
        .await
    }

    async fn message_delete(
        &self,
        _ctx: Context,
        channel_id: ChannelId,
        deleted_message_id: MessageId,
        guild_id: Option<GuildId>,
    ) {
        self.forward(DiscordMessage::MessageDelete {
            channel_id,
            deleted_message_id,
            guild_id,
        })
        .await
    }

    async fn message_delete_bulk(
        &self,
        _ctx: Context,
        channel_id: ChannelId,
        multiple_deleted_messages_ids: Vec<MessageId>,
        guild_id: Option<GuildId>,
    ) {
        self.forward(DiscordMessage::MessageDeleteBulk {
            channel_id,
            multiple_deleted_messages_ids,
            guild_id,
        })
        .await
    }

    async fn message_update(
        &self,
        _ctx: Context,
        old_if_available: Option<Message>,
        new: Option<Message>,
        event: serenity::model::event::MessageUpdateEvent,
    ) {
        self.forward(DiscordMessage::MessageUpdate {
            old_if_available,
            new,
            event,
        })
        .await
    }

    async fn reaction_remove_all(
        &self,
        _ctx: Context,
        channel_id: ChannelId,
        removed_from_message_id: MessageId,
    ) {
        self.forward(DiscordMessage::ReactionRemoveAll {
            channel_id,
            removed_from_message_id,
        })
        .await
    }

    async fn resume(&self, _ctx: Context, resume: ResumedEvent) {
        self.forward(DiscordMessage::Resume(resume)).await
    }

    async fn user_update(&self, _ctx: Context, old_data: CurrentUser, new: CurrentUser) {
        self.forward(DiscordMessage::UserUpdate { old_data, new })
            .await
    }

    async fn voice_server_update(&self, _ctx: Context, update: VoiceServerUpdateEvent) {
        self.forward(DiscordMessage::VoiceServerUpdate(update))
            .await
    }

    async fn voice_state_update(
        &self,
        _ctx: Context,
        _: Option<GuildId>,
        old: Option<VoiceState>,
        new: VoiceState,
    ) {
        self.forward(DiscordMessage::VoiceUpdate { old, new }).await
    }

    async fn webhook_update(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        belongs_to_channel_id: ChannelId,
    ) {
        self.forward(DiscordMessage::WebhookUpdate {
            guild_id,
            belongs_to_channel_id,
        })
        .await
    }
}

#[async_trait::async_trait]
impl Source for Discord {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn reply_event(
        &mut self,
        event: Event,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
    ) -> Result<()> {
        if let Some((tx, _)) = self.client.as_mut() {
            for v in event.value_iter() {
                if let Err(e) = tx.send(v.clone_static()).await {
                    error!("Send error: {}", e);
                }
            }
        }
        Ok(())
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        if let Some((_, rx)) = self.client.as_mut() {
            match rx.try_recv() {
                Ok(data) => {
                    let origin_uri = self.origin_uri.clone();

                    Ok(SourceReply::Structured {
                        origin_uri,
                        data: data.into(),
                    })
                }
                Err(TryRecvError::Empty) => Ok(SourceReply::Empty(100)),
                Err(TryRecvError::Closed) => Err("snot! we got no receiver".into()),
            }
        } else {
            Err("snot! we got no receiver".into())
        }
    }

    async fn init(&mut self) -> Result<SourceState> {
        // by Discord for bot users.
        let token = self.config.token.clone();
        let (tx, rx) = async_channel::bounded(QSIZE);
        let (reply_tx, reply_rx) = async_channel::bounded(QSIZE);
        self.client = Some((reply_tx, rx));
        let client = Client::builder(&token).event_handler(Handler {
            tx,
            rx: reply_rx,
            is_loop_running: AtomicBool::from(false),
        });

        let client = if self.config.intents.is_empty() {
            client
        } else {
            let intents = self
                .config
                .intents
                .iter()
                .copied()
                .map(Intents::into)
                .fold(GatewayIntents::default(), |a, b| a | b);
            client.intents(intents)
        };
        let mut client = client
            .await
            .map_err(|e| Error::from(format!("Err discord creating client: {}", e)))?;
        task::spawn(async move { client.start().await });

        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Discord {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        SourceManager::start(self.clone(), config).await
    }

    fn default_codec(&self) -> &str {
        "string"
    }
}

#[cfg(test)]
mod tests {}
