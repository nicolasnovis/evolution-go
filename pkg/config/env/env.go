package config_env

const (
	POSTGRES_AUTH_DB        = "POSTGRES_AUTH_DB"
	POSTGRES_USERS_DB       = "POSTGRES_USERS_DB"
	POSTGRES_HOST           = "POSTGRES_HOST"
	POSTGRES_PORT           = "POSTGRES_PORT"
	POSTGRES_USER           = "POSTGRES_USER"
	POSTGRES_PASSWORD       = "POSTGRES_PASSWORD"
	POSTGRES_DB             = "POSTGRES_DB"
	DATABASE_SAVE_MESSAGES  = "DATABASE_SAVE_MESSAGES"
	GLOBAL_API_KEY          = "GLOBAL_API_KEY"
	WA_DEBUG                = "DEBUG_ENABLED"
	LOGTYPE                 = "LOG_TYPE"
	WEBHOOKFILES            = "WEBHOOK_FILES"
	CONNECT_ON_STARTUP      = "CONNECT_ON_STARTUP"
	OS_NAME                 = "OS_NAME"
	AMQP_URL                = "AMQP_URL"
	AMQP_GLOBAL_ENABLED     = "AMQP_GLOBAL_ENABLED"
	AMQP_GLOBAL_EVENTS      = "AMQP_GLOBAL_EVENTS"
	AMQP_SPECIFIC_EVENTS    = "AMQP_SPECIFIC_EVENTS"
	WEBHOOK_URL             = "WEBHOOK_URL"
	CLIENT_NAME             = "CLIENT_NAME"
	API_AUDIO_CONVERTER     = "API_AUDIO_CONVERTER"
	API_AUDIO_CONVERTER_KEY = "API_AUDIO_CONVERTER_KEY"
	MINIO_ENDPOINT          = "MINIO_ENDPOINT"
	MINIO_ACCESS_KEY        = "MINIO_ACCESS_KEY"
	MINIO_SECRET_KEY        = "MINIO_SECRET_KEY"
	MINIO_BUCKET            = "MINIO_BUCKET"
	MINIO_USE_SSL           = "MINIO_USE_SSL"
	MINIO_ENABLED           = "MINIO_ENABLED"
	MINIO_REGION            = "MINIO_REGION"
	WHATSAPP_VERSION_MAJOR  = "WHATSAPP_VERSION_MAJOR"
	WHATSAPP_VERSION_MINOR  = "WHATSAPP_VERSION_MINOR"
	WHATSAPP_VERSION_PATCH  = "WHATSAPP_VERSION_PATCH"
	PROXY_PROTOCOL          = "PROXY_PROTOCOL"
	PROXY_HOST              = "PROXY_HOST"
	PROXY_PORT              = "PROXY_PORT"
	PROXY_USERNAME          = "PROXY_USERNAME"
	PROXY_PASSWORD          = "PROXY_PASSWORD"
	NATS_URL                = "NATS_URL"
	NATS_GLOBAL_ENABLED     = "NATS_GLOBAL_ENABLED"
	NATS_GLOBAL_EVENTS      = "NATS_GLOBAL_EVENTS"
	EVENT_IGNORE_GROUP      = "EVENT_IGNORE_GROUP"
	EVENT_IGNORE_STATUS     = "EVENT_IGNORE_STATUS"
	QRCODE_MAX_COUNT        = "QRCODE_MAX_COUNT"
	CHECK_USER_EXISTS       = "CHECK_USER_EXISTS"

	// Freios de memória do download de mídia (ver pkg/mediaguard). Ambos nascem INERTES: 0/vazio =
	// comportamento antigo. Ligar por env, sem rebuild.
	MEDIA_MAX_AUTODOWNLOAD_BYTES = "MEDIA_MAX_AUTODOWNLOAD_BYTES" // pula o auto-download acima de N bytes (0 = sem teto)
	MEDIA_DOWNLOAD_CONCURRENCY   = "MEDIA_DOWNLOAD_CONCURRENCY"   // máx. downloads simultâneos no processo (0 = ilimitado)

	// Outbox durável de webhook (ver pkg/events/webhook/outbox_*). Nasce INERTE: vazio/false = comportamento
	// antigo (fire-and-retry em memória). "true" liga a fila write-ahead no Postgres + worker de drain.
	WEBHOOK_OUTBOX_ENABLED = "WEBHOOK_OUTBOX_ENABLED"

	// Teto de bytes do corpo de UM webhook de HistorySync (ver pkg/whatsmeow/service/history_sync_split).
	// Acima disso o evento é fatiado em N chunks menores (subconjunto de Conversations por chunk) pra não
	// estourar o limite de corpo da plataforma (413 na Vercel > 4,5MB). Vazio/0 = usa o default 3.500.000.
	// Pra DESLIGAR o fatiamento: valor gigante (ex.: "999999999") ou swap reverso.
	WEBHOOK_MAX_PAYLOAD_BYTES = "WEBHOOK_MAX_PAYLOAD_BYTES"

	// Como o aparelho se apresenta no PAREAMENTO ("chrome" padrão, ou "desktop"). Só vai no registro do
	// device — sessões já pareadas não mudam. Existe pra testar se desktop recebe mais histórico sem rebuild.
	PAIRING_PLATFORM = "PAIRING_PLATFORM"

	// Teto de vazão da faixa de HistorySync do outbox, em bytes/s (ver outbox_worker.paceFor). Vazio = default
	// 50000 (~1 chunk de 1,5MB a cada 30s). "0" desliga o teto. Protege o tempo real do CRM de um despejo.
	WEBHOOK_BULK_BYTES_PER_SEC = "WEBHOOK_BULK_BYTES_PER_SEC"

	// Logger configurations
	LOG_MAX_SIZE    = "LOG_MAX_SIZE"
	LOG_MAX_BACKUPS = "LOG_MAX_BACKUPS"
	LOG_MAX_AGE     = "LOG_MAX_AGE"
	LOG_DIRECTORY   = "LOG_DIRECTORY"
	LOG_COMPRESS    = "LOG_COMPRESS"
)
