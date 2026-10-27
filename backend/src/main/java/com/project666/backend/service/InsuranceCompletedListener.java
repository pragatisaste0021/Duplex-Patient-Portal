package com.project666.backend.service;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.project666.backend.config.RabbitConfig;
import com.project666.backend.domain.entity.AppointmentBill;
import com.project666.backend.domain.entity.BillStatusEnum;
import com.project666.backend.domain.entity.LabBill;
import com.project666.backend.domain.message.InsuranceCompletedMessage;
import com.project666.backend.repository.AppointmentBillRepository;
import com.project666.backend.repository.LabBillRepository;
import lombok.extern.slf4j.Slf4j;

import lombok.RequiredArgsConstructor;

@Slf4j
@Service
@RequiredArgsConstructor
public class InsuranceCompletedListener {

    private final AppointmentBillRepository appointmentBillRepository;
    private final LabBillRepository labBillRepository;

    @RabbitListener(queues = RabbitConfig.INSURANCE_COMPLETED_QUEUE)
    public void handleInsuranceCompleted(InsuranceCompletedMessage message) {

        log.info(
            "Received billing.insurance.completed for billId={}, insuranceCover={}, patientPayment={}",
            message.getBillId(),
            message.getInsuranceCoverAmount(),
            message.getPatientPaymentAmount()
        );

        AppointmentBill appointmentBill = appointmentBillRepository.findById(message.getBillId()).orElse(null);
        if (appointmentBill != null) {
            if (!BillStatusEnum.VIEWING.equals(appointmentBill.getStatus())) {
                return;
            }

            appointmentBill.setInsuranceCoverAmount(message.getInsuranceCoverAmount());
            appointmentBill.setPatientPaymentAmount(message.getPatientPaymentAmount());
            appointmentBill.setStatus(BillStatusEnum.UNPAID);
            appointmentBillRepository.save(appointmentBill);
            return;
        }

        LabBill labBill = labBillRepository.findById(message.getBillId()).orElse(null);
        if (labBill != null) {
            if (!BillStatusEnum.VIEWING.equals(labBill.getStatus())) {
                return;
            }

            labBill.setInsuranceCoverAmount(message.getInsuranceCoverAmount());
            labBill.setPatientPaymentAmount(message.getPatientPaymentAmount());
            labBill.setStatus(BillStatusEnum.UNPAID);
            labBillRepository.save(labBill);
        }else throw new IllegalArgumentException(
            String.format("Bill with ID %s not found", message.getBillId())
        );
    }
}
