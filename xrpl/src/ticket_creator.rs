use std::sync::Arc;

use tracing::{debug, error, info, warn};

use crate::config::XRPLConfig;
use relayer_base::gmp_api::{
    gmp_types::{BroadcastRequest, QueryRequest},
    GmpApiTrait,
};
use relayer_base::utils::ThreadSafe;
use xrpl_multisig_prover;

pub struct XrplTicketCreator<G: GmpApiTrait + ThreadSafe> {
    gmp_api: Arc<G>,
    config: XRPLConfig,
}

impl<G> XrplTicketCreator<G>
where
    G: GmpApiTrait + ThreadSafe,
{
    pub fn new(gmp_api: Arc<G>, config: XRPLConfig) -> Self {
        Self { gmp_api, config }
    }

    async fn work(&self) {
        let ticket_create_query =
            serde_json::to_value(xrpl_multisig_prover::msg::QueryMsg::TicketCreate);
        if ticket_create_query.is_err() {
            error!(
                "Failed to serialize TicketCreate query: {:?}",
                ticket_create_query.err()
            );
            return;
        }

        let query_res = self
            .gmp_api
            .post_query(
                self.config
                    .common_config
                    .axelar_contracts
                    .chain_multisig_prover
                    .clone(),
                &QueryRequest::Generic(ticket_create_query.unwrap_or_default()),
            )
            .await;

        match query_res {
            Err(e)
                if e.to_string()
                    .contains("ticket count threshold has not been reached") =>
            {
                debug!("Ticket count threshold has not been reached. Skipping ticket create.");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                return;
            }
            // On unexpected query failures fall through to the broadcast,
            // which remains the source of truth for whether tickets can be created.
            Err(e) => warn!("Failed to query XRPL Ticket Create: {:?}", e),
            Ok(ticket_count) => debug!(
                "TicketCreate query passed: {} tickets to create",
                ticket_count
            ),
        }

        let ticket_create_msg =
            serde_json::to_value(xrpl_multisig_prover::msg::ExecuteMsg::TicketCreate);
        if ticket_create_msg.is_err() {
            error!(
                "Failed to serialize TicketCreate message: {:?}",
                ticket_create_msg.err()
            );
            return;
        }

        let request = BroadcastRequest::Generic(ticket_create_msg.unwrap_or_default());

        let res = self
            .gmp_api
            .post_broadcast(
                self.config
                    .common_config
                    .axelar_contracts
                    .chain_multisig_prover
                    .clone(),
                &request,
            )
            .await;

        let mut sleep_duration = 5;
        if let Err(e) = res {
            if e.to_string()
                .contains("ticket count threshold has not been reached")
            {
                debug!("Ticket count threshold has not been reached. Ignoring.");
            } else {
                error!("Failed to broadcast XRPL Ticket Create request: {:?}", e);
            }
        } else {
            info!("Ticket create submitted: {}", res.unwrap_or_default());
            sleep_duration = 60; // that's usually how long it takes for the tickets to be confirmed
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(sleep_duration)).await;
    }

    pub async fn run(&self) {
        loop {
            info!("XrplTicketCreator is alive.");
            self.work().await;
        }
    }
}
