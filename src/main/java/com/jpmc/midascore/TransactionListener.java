package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.component.IncentivesApiClient;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private final DatabaseConduit databaseConduit;
    private final IncentivesApiClient incentivesApiClient;

    public TransactionListener(DatabaseConduit databaseConduit, IncentivesApiClient incentivesApiClient) {
        this.databaseConduit = databaseConduit;
        this.incentivesApiClient = incentivesApiClient;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
        
        // Validate and process transaction
        Optional<UserRecord> senderOpt = databaseConduit.findUserById(transaction.getSenderId());
        Optional<UserRecord> recipientOpt = databaseConduit.findUserById(transaction.getRecipientId());
        
        // Check if both sender and recipient exist
        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            logger.warn("Invalid transaction: sender or recipient not found");
            return;
        }
        
        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();
        
        // Check if sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Invalid transaction: insufficient balance for sender {}", sender.getName());
            return;
        }
        
        // Get incentive from API
        float incentive = incentivesApiClient.getIncentive(transaction);
        
        // Transaction is valid - update balances
        // Deduct from sender (no incentive deducted)
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        // Add to recipient (amount + incentive)
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive);
        
        // Save updated users
        databaseConduit.save(sender);
        databaseConduit.save(recipient);
        
        // Save transaction record with incentive
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentive);
        databaseConduit.save(record);
        
        logger.info("Transaction processed: {} sent {} to {} (incentive: {})", 
            sender.getName(), transaction.getAmount(), recipient.getName(), incentive);
        
        // Log Wilbur's balance for debugging
        Optional<UserRecord> wilbur = databaseConduit.findUserByName("wilbur");
        if (wilbur.isPresent()) {
            logger.info("Wilbur's current balance: {}", wilbur.get().getBalance());
        }
    }
}
