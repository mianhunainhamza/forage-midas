package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
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

    public TransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
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
        
        // Transaction is valid - update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());
        
        // Save updated users
        databaseConduit.save(sender);
        databaseConduit.save(recipient);
        
        // Save transaction record
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
        databaseConduit.save(record);
        
        logger.info("Transaction processed successfully: {} sent {} to {}", 
            sender.getName(), transaction.getAmount(), recipient.getName());

        // After processing each transaction, you can log all user balances
        Optional<UserRecord> waldorf = databaseConduit.findUserByName("waldorf");
        if (waldorf.isPresent()) {
            logger.info("Waldorf's current balance: {}", waldorf.get().getBalance());
        }
    }
}
