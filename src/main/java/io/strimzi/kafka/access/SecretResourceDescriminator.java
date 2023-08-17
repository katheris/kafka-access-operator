package io.strimzi.kafka.access;

import io.fabric8.kubernetes.api.model.Secret;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ResourceDiscriminator;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.ResourceEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.strimzi.kafka.access.model.KafkaAccess;

import java.util.Optional;

public class SecretResourceDescriminator implements ResourceDiscriminator<Secret, KafkaAccess> {
    @Override
    public Optional<Secret> distinguish(Class<Secret> resource, KafkaAccess kafkaAccess, Context<KafkaAccess> context) {
        InformerEventSource<Secret, KafkaAccess> eventSource = (InformerEventSource<Secret, KafkaAccess>) context.eventSourceRetriever()
                .getResourceEventSourceFor(Secret.class, SecretDependentResource.class.getName());
        return eventSource.get(new ResourceID(kafkaAccess.getMetadata().getName(), kafkaAccess.getMetadata().getNamespace()));
    }
}
