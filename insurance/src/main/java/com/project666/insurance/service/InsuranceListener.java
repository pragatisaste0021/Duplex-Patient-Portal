package com.project666.insurance.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import com.project666.insurance.config.RabbitConfig;
import com.project666.insurance.message.BillCreatedMessage;
import com.project666.insurance.message.InsuranceCompletedMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class InsuranceListener {

    private final RabbitTemplate rabbitTemplate;
    private final Random random = new Random();

    @RabbitListener(queues = RabbitConfig.BILL_CREATED_QUEUE)
    public void handleBillCreated(BillCreatedMessage message) {
        log.info("Received insurance.bill.created for billId={}", message.getBillId());
        int percent = random.nextInt(61);
        BigDecimal insuranceCoverAmount = message.getAmount()
            .multiply(BigDecimal.valueOf(percent))
            .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP);

        BigDecimal patientPaymentAmount = message.getAmount().subtract(insuranceCoverAmount);

        rabbitTemplate.convertAndSend(
            RabbitConfig.BILL_EXCHANGE,
            RabbitConfig.INSURANCE_COMPLETED_ROUTING_KEY,
            new InsuranceCompletedMessage(
                message.getBillId(),
                insuranceCoverAmount,
                patientPaymentAmount
            )
        );

        log.info(
            "Published billing.insurance.completed for billId={}, insuranceCover={}, patientPayment={}",
            message.getBillId(),
            insuranceCoverAmount,
            patientPaymentAmount
        );
    }
}
