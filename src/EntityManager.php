<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\ORM\Swoole;

use Closure;
use DateTimeInterface;
use Doctrine\Common\EventManager;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\LockMode; // phpcs:ignore
use Doctrine\ORM;
use Doctrine\ORM\AbstractQuery; // phpcs:ignore
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\EntityRepository;
use Doctrine\ORM\Internal\Hydration\AbstractHydrator;
use Doctrine\ORM\OptimisticLockException;
use Doctrine\ORM\ORMException;
use Doctrine\ORM\PessimisticLockException;
use Doctrine\ORM\Query\ResultSetMapping;
use Exception;
use Swoole\Coroutine as Co;
use WeakMap;

use function defer;
use function gc_collect_cycles;

final class EntityManager implements EntityManagerInterface
{
    private WeakMap $emStorage;

    public function __construct(protected Closure $emCreatorFn)
    {
        $this->emStorage = new WeakMap();
    }

    public function getWrappedEm() : EntityManagerInterface
    {
        $em = null;

        /**
         * @var EntityManagerInterface $foundEm
         * @var int $cid
         */
        foreach ($this->emStorage->getIterator() as $foundEm => $cid) {
            if (Co::getCid() === $cid) {
                $em = $foundEm;
                break;
            }
        }

        if (! $em instanceof EntityManagerInterface) {
            /** @var EntityManagerInterface $em */
            $em = ($this->emCreatorFn)();
            $this->emStorage->offsetSet($em, Co::getCid());
            /** @psalm-suppress UnusedFunctionCall */
            defer(static function () use ($em) {
                $em->close();
                unset($em);
                gc_collect_cycles();
            });
        }

        return $em;
    }

    /**
     * @param string $className
     */
    public function getClassMetadata($className) : ORM\Mapping\ClassMetadata
    {
        return $this->getWrappedEm()->getClassMetadata($className);
    }

    /**
     * @return ORM\UnitOfWork
     */
    public function getUnitOfWork()
    {
        return $this->getWrappedEm()->getUnitOfWork();
    }

    /**
     * @param string $className
     * @psalm-suppress all
     */
    public function getRepository($className) : EntityRepository
    {
        $metadata            = $this->getClassMetadata($className);
        $repositoryClassName = $metadata->customRepositoryClassName
            ?: $this->getConfiguration()->getDefaultRepositoryClassName();

        return new $repositoryClassName($this, $metadata);
    }

    /**
     * @return Connection
     */
    public function getConnection()
    {
        return $this->getWrappedEm()->getConnection();
    }

    /**
     * @return ORM\Cache|null
     */
    public function getCache()
    {
        return $this->getWrappedEm()->getCache();
    }

    /**
     * @return ORM\Query\Expr
     */
    public function getExpressionBuilder()
    {
        return $this->getWrappedEm()->getExpressionBuilder();
    }

    public function beginTransaction()
    {
        $this->getWrappedEm()->beginTransaction();
    }

    /**
     * @param callable $func
     * @return mixed
     */
    public function transactional($func)
    {
        return $this->getWrappedEm()->wrapInTransaction($func);
    }

    /**
     * @return mixed
     */
    public function wrapInTransaction(callable $func)
    {
        return $this->getWrappedEm()->wrapInTransaction($func);
    }

    public function commit()
    {
        $this->getWrappedEm()->commit();
    }

    public function rollback()
    {
        $this->getWrappedEm()->rollback();
    }

    /**
     * @param string $dql
     * @return ORM\Query
     */
    public function createQuery($dql = '')
    {
        return $this->getWrappedEm()->createQuery($dql);
    }

    /**
     * @param string $name
     * @return ORM\Query
     */
    public function createNamedQuery($name)
    {
        return $this->getWrappedEm()->createNamedQuery($name);
    }

    /**
     * @param string $sql
     * @return ORM\NativeQuery
     */
    public function createNativeQuery($sql, ResultSetMapping $rsm)
    {
        return $this->getWrappedEm()->createNativeQuery($sql, $rsm);
    }

    /**
     * @param string $name
     * @return ORM\NativeQuery
     */
    public function createNamedNativeQuery($name)
    {
        return $this->getWrappedEm()->createNamedNativeQuery($name);
    }

    /**
     * @return ORM\QueryBuilder
     */
    public function createQueryBuilder()
    {
        return $this->getWrappedEm()->createQueryBuilder();
    }

    /**
     * @param string $entityName
     * @param mixed  $id
     * @psalm-param class-string $entityName
     * @return object|null
     * @throws ORM\ORMException
     */
    public function getReference($entityName, $id)
    {
        return $this->getWrappedEm()->getReference($entityName, $id);
    }

    /**
     * @param string $entityName
     * @param mixed  $identifier
     * @return object|null
     * @psalm-suppress ArgumentTypeCoercion
     */
    public function getPartialReference($entityName, $identifier)
    {
        return $this->getWrappedEm()->getPartialReference($entityName, $identifier);
    }

    public function close()
    {
        $this->getWrappedEm()->close();
    }

    /**
     * @param object $entity
     * @param bool   $deep
     */
    public function copy($entity, $deep = false)
    {
        throw new Exception('Copy of entity was deprecated');
    }

    /**
     * @param object                     $entity
     * @param int                        $lockMode
     * @param int|DateTimeInterface|null $lockVersion
     * @psalm-param LockMode::* $lockMode
     * @psalm-return void
     * @throws OptimisticLockException
     * @throws PessimisticLockException
     */
    public function lock($entity, $lockMode, $lockVersion = null)
    {
        $this->getWrappedEm()->lock($entity, $lockMode, $lockVersion);
    }

    /**
     * @return EventManager
     */
    public function getEventManager()
    {
        return $this->getWrappedEm()->getEventManager();
    }

    /**
     * @return ORM\Configuration
     */
    public function getConfiguration()
    {
        return $this->getWrappedEm()->getConfiguration();
    }

    /**
     * @return bool
     */
    public function isOpen()
    {
        return $this->getWrappedEm()->isOpen();
    }

    /** @param int|string $hydrationMode */
    public function getHydrator($hydrationMode)
    {
        throw new Exception('Method getHydrator was deprecated');
    }

    /**
     * @param string|int $hydrationMode
     * @psalm-param string|AbstractQuery::HYDRATE_* $hydrationMode
     * @return AbstractHydrator
     * @throws ORMException
     */
    public function newHydrator($hydrationMode)
    {
        return $this->getWrappedEm()->newHydrator($hydrationMode);
    }

    /**
     * @psalm-suppress DeprecatedClass
     * @return ORM\Proxy\ProxyFactory
     */
    public function getProxyFactory()
    {
        return $this->getWrappedEm()->getProxyFactory();
    }

    /**
     * @return ORM\Query\FilterCollection
     */
    public function getFilters()
    {
        return $this->getWrappedEm()->getFilters();
    }

    /**
     * @return bool
     */
    public function isFiltersStateClean()
    {
        return $this->getWrappedEm()->isFiltersStateClean();
    }

    /**
     * @return bool
     */
    public function hasFilters()
    {
        return $this->getWrappedEm()->hasFilters();
    }

    /**
     * @param string   $className
     * @param mixed    $id
     * @param int|null $lockMode
     * @param int|null $lockVersion
     * @psalm-param class-string $className
     * @psalm-param LockMode::*|null $lockMode
     * @return object|null
     */
    public function find($className, $id, $lockMode = null, $lockVersion = null)
    {
        return $this->getWrappedEm()->find($className, $id, $lockMode, $lockVersion);
    }

    /**
     * @param object $object
     */
    public function persist($object)
    {
        $this->getWrappedEm()->persist($object);
    }

    /**
     * @param object $object
     */
    public function remove($object)
    {
        $this->getWrappedEm()->remove($object);
    }

    /**
     * @param object $object
     */
    public function merge($object) : void
    {
        throw new Exception('Method merge was deprecated');
    }

    public function clear() : void
    {
        $this->getWrappedEm()->clear();
    }

    /**
     * @param object $object
     */
    public function detach($object)
    {
        $this->getWrappedEm()->detach($object);
    }

    /**
     * @param object $object
     */
    public function refresh($object)
    {
        $this->getWrappedEm()->refresh($object);
    }

    public function flush()
    {
        $this->getWrappedEm()->flush();
    }

    /**
     * @return ORM\Mapping\ClassMetadataFactory
     */
    public function getMetadataFactory()
    {
        return $this->getWrappedEm()->getMetadataFactory();
    }

    /**
     * @param object $obj
     */
    public function initializeObject($obj)
    {
        $this->getWrappedEm()->initializeObject($obj);
    }

    /**
     * @param object $object
     * @return bool
     */
    public function contains($object)
    {
        return $this->getWrappedEm()->contains($object);
    }

    public function reopen() : void
    {
        $em = $this->getWrappedEm();
        if ($em->isOpen()) {
            $em->clear();
        } else {
            $this->emStorage->offsetUnset($em);
        }
    }
}
