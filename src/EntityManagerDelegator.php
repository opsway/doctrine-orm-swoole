<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\ORM\Swoole;

use Closure;
use Psr\Container\ContainerInterface;

final class EntityManagerDelegator
{
    public function __invoke(ContainerInterface $container, string $name, Closure $emCreatorFn) : EntityManager
    {
        return new EntityManager($emCreatorFn);
    }
}
